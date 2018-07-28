-- Update File C records to null out any references to dummy awards
UPDATE financial_accounts_by_awards
SET award_id = NULL
WHERE award_id = ANY(SELECT id FROM awards WHERE UPPER(generated_unique_award_id) = 'NONE');

DELETE
FROM awards
WHERE UPPER(generated_unique_award_id) = 'NONE';