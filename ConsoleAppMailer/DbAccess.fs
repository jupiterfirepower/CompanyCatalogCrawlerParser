module DbAccess

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open DomainModel

[<Literal>]
let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=CountryDictionary;Integrated Security=True;Connect Timeout=3;"
type sql = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER,
                           connectionString,
                           IndividualsAmount = 1000,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let getEmails =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = IsolationLevel.DontCreateTransaction; 
                               FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0) }
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
       for email in ctx.Dbo.TblEmail do
       sortBy email.EmailId
       select email
       } |> Seq.map (fun x -> Email(x.Email, x.EmailId, x.BadIgnoreEmail) )

let updateReportSendEmailId (emailFileName, emailId) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = IsolationLevel.DontCreateTransaction; 
                               FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)

    let qry = 
        query {
            for reportSendedEmail in ctx.Dbo.TblEmailSendedReport do
            where (reportSendedEmail.EmailFileName = emailFileName)
        }
    if (Seq.isEmpty qry) then
       let report = ctx.Dbo.TblEmailSendedReport
       let row = report.Create()
       row.EmailFileName <- emailFileName
       row.LastEmailIdSended <- emailId
       ctx.SubmitUpdates()
    else
      qry |> Seq.iter( fun e ->
          e.LastEmailIdSended <- emailId
      )
      ctx.SubmitUpdates()

let getReportLastEmailIdForFileName(emailFileName) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = IsolationLevel.DontCreateTransaction; 
                               FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0) }
    let ctx = sql.GetDataContext(TransactionOptions)
    let qry = 
        query {
           for reportSendedEmail in ctx.Dbo.TblEmailSendedReport do
           where (reportSendedEmail.EmailFileName = emailFileName)
           select reportSendedEmail
       } 
    if not(Seq.isEmpty qry) then
        (qry |> Seq.last).LastEmailIdSended
    else
        0

let updateBadIgnoreEmail(emailId, flag) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = IsolationLevel.DontCreateTransaction; 
                               FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0) }
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
              for email in ctx.Dbo.TblEmail do
              where (email.EmailId = emailId)
              select email
    } |>       
    Seq.iter( fun e ->
        e.BadIgnoreEmail <- flag
    )
    ctx.SubmitUpdates()

