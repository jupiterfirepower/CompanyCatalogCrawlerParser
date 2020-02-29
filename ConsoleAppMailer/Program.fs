// Learn more about F# at http://fsharp.org

open System.Text
open EAGetMail
open System.Net.Mail
open FSharp.Data.Sql
open FSharp.Collections.ParallelSeq
open FSharp.Data
open DataStructure.Helpers
open DomainModel
open DbAccess

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

                             let doc = HtmlDocument.Parse(oMail.HtmlBody)
                             resizeImagesInDoc(doc, rootDirForEmailImages)

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
                                                           let fileName = sprintf @"%s\%s" rootDirForEmailImages cur.Name
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
                                 let fileName = sprintf @"%s\%s" rootDirForEmailImages cur.Name
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
                                                                                    let fileName = sprintf @"%s\%s" rootDirForEmailImages cur.Name
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
