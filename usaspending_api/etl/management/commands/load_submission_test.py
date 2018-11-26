from datetime import datetime
import logging
import re
import signal

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connections, transaction

from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.accounts.models import (
    AppropriationAccountBalances, AppropriationAccountBalancesQuarterly, TreasuryAppropriationAccount)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass, TasProgramActivityObjectClassQuarterly)

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.helpers import get_fiscal_quarter, get_previous_submission
from usaspending_api.etl.management import load_base
from usaspending_api.etl.management.load_base import load_data_into_model

# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't keep hitting the databroker DB for
# account data
TAS_ID_TO_ACCOUNT = {}

logger = logging.getLogger('console')


class Command(load_base.Command):
    """
    This command will load a single submission from the DATA Act broker. If we've already loaded the specified broker
    submission, this command will remove the existing records before loading them again.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable \
                must set so we can pull submission data from their db."

    def add_arguments(self, parser):
        parser.add_argument('submission_id', nargs=1, help='the data broker submission id to load', type=int)
        super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):
        """ Stuff goes here """

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception('Received interrupt signal. Aborting...')

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        submission_id = options['submission_id'][0]

        logger.info('Getting submission {} from broker...'.format(submission_id))
        db_cursor.execute('SELECT * FROM submission WHERE submission_id = %s', [submission_id])

        submission_data = dictfetchall(db_cursor)
        logger.info('Finished getting submission {} from broker'.format(submission_id))

        if len(submission_data) == 0:
            raise CommandError('Could not find submission with id ' + str(submission_id))

        submission_data = submission_data[0].copy()
        broker_submission_id = submission_data['submission_id']
        submission_status_id = submission_data['publish_status_id']
        del submission_data['submission_id']  # We use broker_submission_id, submission_id is our own PK
        submission_attributes = get_submission_attributes(broker_submission_id, submission_data)

        logger.info('Getting File A data')
        db_cursor.execute('SELECT * FROM appropriation WHERE submission_id = %s', [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        logger.info('Acquired File A (appropriation) data for ' + str(submission_id) + ', there are ' + str(
            len(appropriation_data)) + ' rows.')
        logger.info('Loading File A data')
        start_time = datetime.now()
        load_file_a(submission_attributes, appropriation_data, db_cursor)
        logger.info('Finished loading File A data, took {}'.format(datetime.now() - start_time))

        logger.info('Successfully loaded broker submission {}.'.format(options['submission_id'][0]))


def get_submission_attributes(broker_submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending submission record or create and
    return a new one.
    """
    # check if we already have an entry for this broker submission id; if not, create one
    sub_attributes, created = SubmissionAttributes.objects.get_or_create(broker_submission_id=broker_submission_id)

    if created:
        # this is the first time we're loading this broker submission
        logger.info('Creating broker submission id {}'.format(broker_submission_id))

    else:
        # we've already loaded this broker submission, so delete it before reloading if there's another submission that
        # references this one as a "previous submission" do not proceed.
        # TODO: now that we're chaining submissions together, get clarification on what should happen when a submission
        # in the middle of the chain is deleted

        TasProgramActivityObjectClassQuarterly.refresh_downstream_quarterly_numbers(sub_attributes.submission_id)

        logger.info('Broker submission id {} already exists. It will be deleted.'.format(broker_submission_id))
        call_command('rm_submission', broker_submission_id)

    logger.info("Merging CGAC and FREC columns")
    submission_data["cgac_code"] = submission_data["cgac_code"]\
        if submission_data["cgac_code"] else submission_data["frec_code"]

    # Find the previous submission for this CGAC and fiscal year (if there is one)
    previous_submission = get_previous_submission(
        submission_data['cgac_code'],
        submission_data['reporting_fiscal_year'],
        submission_data['reporting_fiscal_period'])

    # Update and save submission attributes
    field_map = {
        'reporting_period_start': 'reporting_start_date',
        'reporting_period_end': 'reporting_end_date',
        'quarter_format_flag': 'is_quarter_format',
    }

    # Create our value map - specific data to load
    value_map = {
        'broker_submission_id': broker_submission_id,
        'reporting_fiscal_quarter': get_fiscal_quarter(submission_data['reporting_fiscal_period']),
        'previous_submission': None if previous_submission is None else previous_submission,
        # pull in broker's last update date to use as certified date
        'certified_date': submission_data['updated_at'].date() if type(
            submission_data['updated_at']) == datetime else None,
    }

    return load_data_into_model(
        sub_attributes, submission_data,
        field_map=field_map, value_map=value_map, save=True)


def load_file_a(submission_attributes, appropriation_data, db_cursor):
    """
    Process and load file A broker data (aka TAS balances, aka appropriation account balances).
    """
    reverse = re.compile('gross_outlay_amount_by_tas_cpe')

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}

    # Create account objects
    for row in appropriation_data:

        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
            row.get('tas_id'), db_cursor)
        if treasury_account is None:
            update_skipped_tas(row, tas_rendering_label, skipped_tas)
            continue

        # Now that we have the account, we can load the appropriation balances
        # TODO: Figure out how we want to determine what row is overridden by what row
        # If we want to correlate, the following attributes are available in the data broker data that might be useful:
        # appropriation_id, row_number appropriation_balances = somethingsomething get appropriation balances...
        appropriation_balances = AppropriationAccountBalances()

        value_map = {
            'treasury_account_identifier': treasury_account,
            'submission': submission_attributes,
            'reporting_period_start': submission_attributes.reporting_period_start,
            'reporting_period_end': submission_attributes.reporting_period_end
        }

        field_map = {}

        load_data_into_model(appropriation_balances, row, field_map=field_map, value_map=value_map, save=True,
                             reverse=reverse)

    AppropriationAccountBalances.populate_final_of_fy()

    # Insert File A quarterly numbers for this submission
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers(submission_attributes.submission_id)

    for key in skipped_tas:
        logger.info('Skipped %d rows due to missing TAS: %s', skipped_tas[key]['count'], key)

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]['count']

    logger.info('Skipped a total of {} TAS rows for File A'.format(total_tas_skipped))


def get_treasury_appropriation_account_tas_lookup(tas_lookup_id, db_cursor):
    """Get the matching TAS object from the broker database and save it to our running list."""
    if tas_lookup_id in TAS_ID_TO_ACCOUNT:
        return TAS_ID_TO_ACCOUNT[tas_lookup_id]
    # Checks the broker DB tas_lookup table for the tas_id and returns the matching TAS object in the datastore
    db_cursor.execute("SELECT * FROM tas_lookup WHERE (financial_indicator2 <> 'F' OR financial_indicator2 IS NULL) "
                      "AND account_num = %s", [tas_lookup_id])
    tas_data = dictfetchall(db_cursor)

    if tas_data is None or len(tas_data) == 0:
        return None, 'Account number {} not found in Broker'.format(tas_lookup_id)

    tas_rendering_label = TreasuryAppropriationAccount.generate_tas_rendering_label(
        ata=tas_data[0]["allocation_transfer_agency"],
        aid=tas_data[0]["agency_identifier"],
        typecode=tas_data[0]["availability_type_code"],
        bpoa=tas_data[0]["beginning_period_of_availa"],
        epoa=tas_data[0]["ending_period_of_availabil"],
        mac=tas_data[0]["main_account_code"],
        sub=tas_data[0]["sub_account_code"]
    )

    TAS_ID_TO_ACCOUNT[tas_lookup_id] = (TreasuryAppropriationAccount.objects.
                                        filter(tas_rendering_label=tas_rendering_label).first(), tas_rendering_label)
    return TAS_ID_TO_ACCOUNT[tas_lookup_id]


def update_skipped_tas(row, tas_rendering_label, skipped_tas):
    if tas_rendering_label not in skipped_tas:
        skipped_tas[tas_rendering_label] = {}
        skipped_tas[tas_rendering_label]['count'] = 1
        skipped_tas[tas_rendering_label]['rows'] = [row['row_number']]
    else:
        skipped_tas[tas_rendering_label]['count'] += 1
        skipped_tas[tas_rendering_label]['rows'] += [row['row_number']]
