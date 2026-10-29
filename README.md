# Transaction Guarantee Reporting

A Scala/Spark project that converts eligible transactions to EUR, allocates
amounts to guarantors and reports totals/averages by country and transaction class.
The Dataset API and SQL paths share the business rules and use different aggregations.

## Run and verify

Use JDK 17 and sbt. Run the local Spark suite without a database:

```sh
sbt 'testOnly com.alitariverdy.FinancialRulesSpec com.alitariverdy.BusinessSpec com.alitariverdy.DataSpec'
```

For the synthetic end-to-end example, start the checked-in PostgreSQL fixture:

```sh
docker compose -f src/test/resources/docker-compose.yml up -d
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=docker
sbt 'runMain com.alitariverdy.SparkApiReport 2025-02-25'
# Same date through the SQL aggregation (replaces that date):
sbt 'runMain com.alitariverdy.SparkSqlReport 2025-02-25'
```

The fixture contains synthetic transactions, a receiver registry and exchange
rates. Configure the JDBC URL in `src/main/resources/application.conf` when using
a different database. JDK module options in `build.sbt` apply to tests; use a
Spark distribution's launcher for standalone production jobs.

## Example evidence

For an eligible USD 10.01 transaction with rate 0.8 and shares 0.1/0.9, the
allocated EUR amounts are 0.8008 and 7.2072. `FinancialRulesSpec` asserts those
amounts independently and compares both aggregation results. This intentionally
shows precision before a business-specific rounding policy is introduced.

## Structure

- `data/rows`: typed inputs, allocations and report rows.
- `data/*Repository`: dated source selection.
- `business`: eligibility, reference validation, conversion and allocation.
- `db/SQLs.scala`: SQL aggregation over shared allocated transactions.
- `db/PostgresConnector.scala`: JDBC reads and staged report publication.

## Financial rules and reproducible evidence

Run `sbt 'testOnly com.alitariverdy.FinancialRulesSpec'` on JDK 17. This local
Spark suite needs no PostgreSQL or Docker. It checks independent expected decimal
amounts, SQL/Dataset parity, missing rates, duplicate rates, and invalid shares.

Both entrypoints now share eligibility, EUR conversion and decimal allocation;
their aggregation implementations remain separate. Rates use the latest global
batch on or before the report date, not a different date per currency. Each
transaction currency requires one positive EUR rate, including EUR itself when
present. Incomplete or duplicate batches fail before the join instead of dropping
transactions or choosing an arbitrary duplicate. Output currency is EUR.

Shares are decimal fractions in [0,1], with non-empty unique guarantor names and
sum at most 1 (partial guarantees are allowed). Invalid JSON fails the batch.
There is no additional rounding before aggregation; Spark decimal precision and
scale still apply. Production use requires an agreed currency rounding policy.

Report entrypoints stage Spark output in PostgreSQL, then atomically replace the
requested date in a transaction. Reruns replace that date; other dates remain.
An insert failure rolls back the delete. Publishing takes a table lock to serialize
writers and requires CREATE/DROP permissions in the target schema. A killed process
can leave an orphan staging table for cleanup; it cannot expose that stage as the
published report. The low-level `writeTable` method still explicitly appends.

`ReportPublishSpec` uses a real PostgreSQL container to test replacement, preservation
of other dates, rollback on a constraint failure, and clearing an empty report date.
Run it with Docker using `sbt 'testOnly com.alitariverdy.ReportPublishSpec'`.
This is a portfolio implementation, not a claim of regulatory certification.
