// Learn more about F# at http://fsharp.org

open System
open System.Text
open EAGetMail
open System.Net.Mail
open System.Threading
open System.IO
open FSharp.Data.Sql
open FSharp.Collections.ParallelSeq
open FSharp.Data.Sql.Transactions

let server   = "smtp.elasticemail.com" 
let sender   = "ElasticEmailAccount" //account when you registered
let password = "ElasticEmailGeneratedCode" // generated code password for app
let port = 2525

//smtp.elasticemail.com
let sendMailMessage sendtoemail subject msg attachments (ccemails:seq<#MailAddress>) (bccemails:seq<#MailAddress>) = 
    use msg = new MailMessage(sender, sendtoemail, subject, msg)
    msg.IsBodyHtml <- true

    for cc in ccemails do
        msg.CC.Add(cc)

    for bcc in bccemails do
        msg.CC.Add(bcc)

    for attachment in attachments do
        msg.Attachments.Add(attachment)

    use client = new SmtpClient(server, port)
    client.DeliveryFormat <- SmtpDeliveryFormat.International
    //client.EnableSsl <- true
    client.UseDefaultCredentials <- false
    client.Credentials <- System.Net.NetworkCredential(sender, password)
    client.Send(msg)

type MutableList<'item when 'item:equality>(init) =
    let mutable items: 'item list = init

    member x.Value = items

    member x.Update updater =
        let current = items
        let newItems = updater current
        if not <| obj.ReferenceEquals(current, Interlocked.CompareExchange(&items, newItems, current))
            then x.Update updater
            else x

    member x.Add item = x.Update (fun L -> item::L)
    member x.Remove item = x.Update (fun L -> List.filter (fun i -> i <> item) L)
    member x.Contains item = let current = items |> List.map(fun x -> x)
                             List.contains item current

    static member empty = new MutableList<'item>([])
    static member add item (l:MutableList<'item>) = l.Add item
    static member get (l:MutableList<'item>) = l.Value
    static member remove item (l:MutableList<'item>) = l.Remove item

[<Literal>]
let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=CountryDictionary;Integrated Security=True;Connect Timeout=3;"
type sql = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER,
                           connectionString,
                           IndividualsAmount = 1000,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>
[<Struct>]
type Email = 
     val mutable Email: string
     val mutable EmailId : int
     val mutable BadIgnoreEmail : bool
     new(email: string, emailid:int, badignoreemail:bool) = { Email = email; EmailId = emailid; BadIgnoreEmail = badignoreemail }
        

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

let rec allFiles dirs =
    if Seq.isEmpty dirs then Seq.empty else
        seq { yield! dirs |> Seq.collect Directory.EnumerateFiles
              yield! dirs |> Seq.collect Directory.EnumerateDirectories |> allFiles }

let getAllFilesFromDir path =
    Directory.EnumerateFiles(path, "*.msg", SearchOption.AllDirectories)

type RetryBuilder(max, sleep : TimeSpan) = 
    member x.Return(a) = a
    member x.Delay(f) = f
    member x.Zero() = x.Return(())
    member x.Run(f) =
      let rec loop(n) = 
          if n = 0 then failwith "Failed"
          else 
              try f() 
              with ex -> 
                  sprintf "Call failed with %s. Retrying." ex.Message |> printfn "%s"
                  System.Threading.Thread.Sleep(sleep); 
                  loop(n-1)
      loop max

let rec deleteFiles srcPath pattern includeSubDirs =
    
    if not <| System.IO.Directory.Exists(srcPath) then
        let msg = System.String.Format("Source directory does not exist or could not be found: {0}", srcPath)
        raise (System.IO.DirectoryNotFoundException(msg))

    for file in System.IO.Directory.EnumerateFiles(srcPath, pattern) do
        let tempPath = System.IO.Path.Combine(srcPath, file)
        System.IO.File.Delete(tempPath)

    if includeSubDirs then
        let srcDir = new System.IO.DirectoryInfo(srcPath)
        for subdir in srcDir.GetDirectories() do
            deleteFiles subdir.FullName pattern includeSubDirs

[<Literal>]
let rootDirForEmail = @"E:\EMAILTMP"
[<Literal>]
let rootDirForEmailImages = @"E:\EMAILTMP\TMPIMG"

[<EntryPoint>]
let main _ =
    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance)

    let badEmails = MutableList<Email>.empty
    let sourceEmails = getEmails |> Seq.distinctBy(fun e -> e.Email)
    let emailFileNames = getAllFilesFromDir rootDirForEmail

    let emailFileNames = emailFileNames |> List.ofSeq
    emailFileNames
    |> PSeq.iter (fun efn -> let lastEmailId = getReportLastEmailIdForFileName(efn) 
                             let filteredEmails = sourceEmails |> Seq.where (fun eml -> eml.EmailId > lastEmailId && not(eml.BadIgnoreEmail))
                             let mutable lastEmail:Email = Unchecked.defaultof<_>
                             let count = ref 0

                             let oMail = new Mail("TryIt")
                             oMail.LoadOMSG(efn)

                             deleteFiles rootDirForEmailImages "*.*" false

                             // Parse Attachments
                             let atts = oMail.Attachments
                             for cur in atts do
                                 printfn "Att: %s" cur.Name 
                                 let fileName = sprintf @"%s\%s" rootDirForEmailImages cur.Name
                                 cur.SaveAs(fileName, true)

                             filteredEmails
                             |> Seq.take 1000
                             |> Seq.iter (fun e -> let ccEmails = MutableList<System.Net.Mail.MailAddress>.empty
                                                   let bccEmails = MutableList<System.Net.Mail.MailAddress>.empty
                                                   let attachments = MutableList<System.Net.Mail.Attachment>.empty
                                                   
                                                   try
                                                       let mutable currentEmail:string=null

                                                       currentEmail <- e.Email.Replace("%40","@")
                                                       if e.Email.StartsWith("nfo@") then
                                                           currentEmail <- e.Email.Replace("nfo@","info@")

                                                       let cemail = MailAddress(currentEmail)
                                                       let emailSubject = oMail.Subject.Replace("(Trial Version)","")

                                                       // Parse Attachments
                                                       let atts = oMail.Attachments
                                                       for cur in atts do
                                                           let fileName = sprintf @"E:\EMAILTMP\TMPIMG\%s" cur.Name
                                                           let attachment = new System.Net.Mail.Attachment(fileName)
                                                           attachment.Name <- cur.Name
                                                           attachment.ContentId <- cur.ContentID
                                                           attachments.Add(attachment) |> ignore

                                                       count := !count + 1
                                                       
                                                       lastEmail <- e
                                                       sendMailMessage cemail.Address emailSubject oMail.HtmlBody attachments.Value ccEmails.Value bccEmails.Value 
                                                       printfn "Email sended to %s EmailId: %d  (%d) OK." cemail.Address lastEmail.EmailId !count

                                                       updateReportSendEmailId(efn, lastEmail.EmailId)
                                                   with
                                                       | _ as ex -> printfn "path - %s error:  %s lastEmailId - %d" efn ex.Message lastEmail.EmailId
                                                                    badEmails.Add(e) |> ignore
                                          )
                 )
    badEmails.Value |> Seq.iter (fun eml -> updateBadIgnoreEmail(eml.EmailId, true))
    

    (*let count = ref 0
    let badEmails = MutableList<Email>.empty
    let sourceEmails = getEmails
    let emailFileNames = getAllFilesFromDir @"E:\EMAILTMP"

    let emailFileNames = emailFileNames |> List.ofSeq
    emailFileNames
    |> PSeq.iter (fun efn -> let lastEmailId = getReportLastEmailIdForFileName(efn) 
                             let filteredEmails = sourceEmails |> Seq.where (fun eml -> eml.EmailId > lastEmailId && not(eml.BadIgnoreEmail))

                             let oMail = new Mail("TryIt")
                             oMail.LoadOMSG(efn)

                             // Parse Attachments
                             let atts = oMail.Attachments
                             for cur in atts do
                                 let fileName = sprintf @"E:\EMAILTMP\TMPIMG\%s" cur.Name
                                 cur.SaveAs(fileName, true)

                             filteredEmails
                             |> Seq.take 60
                             |> Seq.chunkBySize 10
                             |> Seq.iter (fun egrp -> let mutable lastEmail:Email = Unchecked.defaultof<_>
                                                      let lastGroupEmail = egrp |> Seq.last

                                                      egrp 
                                                      |> PSeq.iter (fun e -> let ccEmails = MutableList<System.Net.Mail.MailAddress>.empty
                                                                             let bccEmails = MutableList<System.Net.Mail.MailAddress>.empty
                                                                             let attachments = MutableList<System.Net.Mail.Attachment>.empty
                                                      
                                                                             try
                                                                                let currentEmail = e.Email.Replace("%40","@")
                                                                                let cemail = MailAddress(currentEmail)
                                                                                let emailSubject = oMail.Subject.Replace("(Trial Version)","")

                                                                                // Parse Attachments
                                                                                let atts = oMail.Attachments
                                                                                for cur in atts do
                                                                                    let fileName = sprintf @"E:\EMAILTMP\TMPIMG\%s" cur.Name
                                                                                    let attachment = new System.Net.Mail.Attachment(fileName)
                                                                                    attachment.Name <- cur.Name
                                                                                    attachment.ContentId <- cur.ContentID
                                                                                    attachments.Add(attachment) |> ignore

                                                                                count := !count + 1
                                                                                lastEmail <- e

                                                                                sendMailMessage cemail.Address emailSubject oMail.HtmlBody attachments.Value ccEmails.Value bccEmails.Value
                                                                                printfn "Email sended to %s EmailId: %d  (%d) OK." cemail.Address lastEmail.EmailId !count
                                                                                
                                                                             with
                                                                                 | _ as ex -> printfn "path - %s error:  %s lastEmailId - %d" efn ex.Message lastEmail.EmailId
                                                                                              badEmails.Add(e) |> ignore
                                                                    )
                                                      
                                                      updateReportSendEmailId(efn, lastGroupEmail.EmailId)
                                          ) 
                 )
                             
    badEmails.Value |> Seq.iter (fun eml -> updateBadIgnoreEmail(eml.EmailId, true)) *)
   
    0 // return an integer exit code
