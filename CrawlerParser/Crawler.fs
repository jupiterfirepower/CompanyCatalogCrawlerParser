module Crawler

open System
open FSharp.Data
open FSharp.Data.Sql
open FSharp.Text.RegexProvider
open FSharp.Data.HttpRequestHeaders
open System.Net
open System.Collections.Concurrent
open System.Threading
open System.Net.Http
open System.Text
open Utils
open DomainModel
open DbAccess

let links (html:HtmlDocument, tagName: string) = 
    html.Descendants [tagName]
    |> Seq.choose (fun x -> x.TryGetAttribute("href")
                            |> Option.map (fun a -> x.InnerText(), a.Value())
                   )   
                   
let hrefLinks (html:HtmlDocument) = 
    let href = links(html, "a")
    let link = links(html, "link")
    let area = links(html, "area")
    let base_ = links(html, "base")
    Seq.append href link |> Seq.append area |> Seq.append base_

let startHttp(x:string)=
    (x.StartsWith("http://") || x.StartsWith("https://"))

let isNotImageExt(url:string) = 
    let imgExt = [".png";".jpg";".jpeg";".jfif";".bmp";".gif";".tiff";".tif";".webp";".js";".css"]
    imgExt |> List.forall(fun ext -> not(url.ToLower().EndsWith(ext)))

let notSocialNetwork(url:string) =
    let socNetwork = ["facebook.com";"twitter.com";"pinterest.com";"linkedin.com";"instagram.com";"youtube.com";"vk.com"]
    socNetwork |> List.forall(fun weba -> not(url.Contains(weba)))

let convertUrl(x:string, baseUrl:string)=
    let url =
         if not(startHttp(x)) && x.StartsWith("/") && not(x.StartsWith("//")) then
            sprintf "%s%s" (baseUrl.TrimEnd("/".ToCharArray())) x 
         elif not(startHttp(x)) && not(x.StartsWith("/")) && not(x.StartsWith("//")) then
            sprintf "%s%s" baseUrl x
         else x
    let url = if url.StartsWith("//") then url.Replace("//","http://") else url
    url

let FindLinksRegExp(html:string, baseUrl:string) =
    let baseUrl = if baseUrl.EndsWith("/") then baseUrl else baseUrl+"/"
    let host = getHost(baseUrl)
    //Find all links.
    let regexp = System.Text.RegularExpressions.Regex("<a.*?href=[\"'](?<url>.*?)[\"'].*?>(?<name>.*?)</a>", 
                                                      (System.Text.RegularExpressions.RegexOptions.IgnoreCase 
                                                       ||| System.Text.RegularExpressions.RegexOptions.Compiled 
                                                       ||| System.Text.RegularExpressions.RegexOptions.Singleline), regExpSpanTimeOut) 
    regexp.Matches(html)
    |> Seq.map(fun x -> (x.Groups.[2].Value, x.Groups.[1].Value))
    |> Seq.map (fun (name, x) -> name, convertUrl(x, baseUrl) )
    |> Seq.filter (fun (_, url) -> url.Contains(host) && notSocialNetwork(url) && isNotImageExt(url) )
    |> Seq.distinct

let searchLinkResults(html:HtmlDocument, baseUrl:string) =
    let baseUrl = if baseUrl.EndsWith("/") then baseUrl else baseUrl+"/"
    let host = getHost(baseUrl)
    let data = hrefLinks html
    data
    |> Seq.map (fun (name, x) -> name,  convertUrl(x, baseUrl) )
    |> Seq.filter (fun (_, url) -> url.Contains(host) && notSocialNetwork(url) && isNotImageExt(url)  )
    |> Seq.distinct


type MultipleEmailRegex = Regex< @"\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*", noMethodPrefix = true >
type EmailRegex = Regex< "(?:href)=[\"|']?(.*?)[\"|'|>]+", noMethodPrefix = true >


let countSubstring (where :string) (what : string) =
    match what with
    | "" -> 0 // just a definition; infinity is not an int
    | _ -> (where.Length - where.Replace(what, @"").Length) / what.Length


let companyEmailBag  =  ConcurrentBag<CompanyEmail>()

let contactPage(url:string) =
    let contactsPage = [ "contact";"kontakt";"contac";"kont";"cont";"контакт";
                         "contact-us";"contact-me";"about";"about-us";"about-me";"home";
                         "feedback";"media";"event";"info";"mail";"sendmail"
                         //additional
                         "ofis"; "address";]

    let isContactPage = contactsPage |> List.tryFind(fun cpw -> url.ToLower().Contains(cpw))
    match isContactPage with
    | Some(_) -> true
    | None -> false

let isContactsPage(x:string)=
    let str = x.TrimEnd(@"/".ToCharArray())
    let lastIndex = str.LastIndexOf @"/"
    let length = str.Length - lastIndex - 1
    if lastIndex > 0 && length > 0 then
       let substr = str.Substring(lastIndex + 1, length)
       contactPage(substr)
    elif str.Length > 4 then
       contactPage(str)
    else
       false

let strContainsOnlyNumber (s:string) = s |> Seq.forall Char.IsDigit

let isEndWithDigits(x:string)=
    let str = x.TrimEnd(@"/".ToCharArray())
               .TrimEnd(@".html".ToCharArray())
               .TrimEnd(@".htm".ToCharArray())
    let lastIndex = str.LastIndexOf @"/"
    let length = str.Length - lastIndex - 1
    if lastIndex > 0 && length > 0 then
       let substr = str.Substring(lastIndex + 1, length)
       strContainsOnlyNumber(substr)
    else
       str.ToCharArray().[str.Length-1] |> Char.IsDigit

let searchMailToResults(html:string) =
    let hrefs = EmailRegex().Matches(html)
    let emails = 
        hrefs 
        |> Seq.map (fun x -> x.Value.TrimStart("href=".ToCharArray()).Trim("\"".ToCharArray()).TrimEnd("\">".ToCharArray()) )
        |> Seq.filter (fun x -> x.ToLower().StartsWith("mailto:"))
        |> Seq.map (fun x -> x.ToLower().Replace("mailto:",""))
        |> Seq.distinct
    emails

let searchHrefResults(html:string, baseUrl:string) =
    let baseUrl = if baseUrl.EndsWith("/") then baseUrl else baseUrl+"/"
    let host = getHost(baseUrl)  

    // urlNotContains candidate for use ML.NET
    // urlNotContains(x) 
    let urlNotContains(url:string) =
        let words = ["?";"%";"~";"catalog";"category";"topic";"template";"brend";"node";"goods";
        "drivers";"help";"news";"product";"promo";"sites";"service";"/info/";
        "novosti";"konferenc";"archive";"apartments";"blog";"print";"center";"subscription";
        "province";"item";"feed";"tag";"desc";"search";"shop";"courses";"snap";"albums";
        "show";"photo";"presentation";"clients";"articles";"business";"project";
        "dokument";"image";"proekt";"formy";"kursy";"lists";"biz";"directory";
        "places";"api";"katalog";"index";"bank";"press";"css";"donate";"respond";
        "cat";"online";"page";"comment";"stil";"design";"privacy";"watch";"store";
        "before";"after";"brands";"rieltor";"onlayn";"cars";"options";"results";
        "video";"conditions";"?url";"porn";"teacher";"card";"letters";"obrazovanie";
        "arenda";"estates" ]
        words |> List.forall(fun w -> not(url.ToLower().Contains(w)))


    EmailRegex().Matches(html)
    |> Seq.map (fun x -> x.Value.TrimStart("href=".ToCharArray()).Trim("\"".ToCharArray()).TrimEnd("\">".ToCharArray()) )
    |> Seq.filter (fun x -> x.Length >= 5)
    |> Seq.filter (fun x -> not(x.ToLower().StartsWith("mailto:")))
    |> Seq.filter (fun x -> isNotImageExt(x) && (isContactsPage(x) || (x.Contains("?") && x.Contains("topmenu"))) )

    |> Seq.filter (fun x -> x.EndsWith(".html") || x.EndsWith(".htm") 
                                || ( not(x.EndsWith(".html")) && not(x.EndsWith(".htm")) && x.ToCharArray().[x.Length-4] <> '.' && x.ToCharArray().[x.Length-5] <> '.' ) 
                                || isContactsPage(x)  
                                )
    |> Seq.filter (fun x -> x.EndsWith(".html") || x.EndsWith(".htm") 
                                || x.EndsWith("/") || x.StartsWith("/") 
                                || startHttp(x)
                                || isContactsPage(x) ) 
    |> Seq.distinct
    |> Seq.map (fun x ->  if not(startHttp(x)) && x.StartsWith("/") && not(x.StartsWith("//")) then
                                           sprintf "%s%s" (baseUrl.TrimEnd("/".ToCharArray())) x 
                                         elif not(startHttp(x)) && not(x.StartsWith("/")) && not(x.StartsWith("//")) then
                                           sprintf "%s%s" baseUrl x
                                         else x)
    |> Seq.filter  (fun url -> url.Contains(host)  )
    |> Seq.map (fun url -> if url.StartsWith("//") then 
                              url.Replace("//","http://") 
                           else 
                              url)
    |> Seq.filter (fun url -> countSubstring url "://" = 1)
    |> Seq.filter (fun url -> not ( url.Length = baseUrl.Length || url.Length = (baseUrl.Replace("http://","https://")).Length ))
    |> Seq.filter (fun url -> not ( url.EndsWith("//") ))
    |> Seq.filter ( fun x -> notSocialNetwork(x) )
    |> Seq.filter (fun x -> isEndWithDigits(x) |> not)
    |> Seq.filter (fun x -> x.Length <= 80) 
    |> Seq.distinct

let getHtmlAsStringAsync(url:string) = 
    let linkRedirection = ConcurrentCollections.ConcurrentHashSet<string>()
    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance)
    ServicePointManager.Expect100Continue <- false;
    ServicePointManager.SecurityProtocol <- SecurityProtocolType.Tls13 ||| SecurityProtocolType.Tls12 ||| SecurityProtocolType.Tls11 ||| SecurityProtocolType.Tls;
    
    let rec getHtmlAsync (url:string) = 
        async {
            use httpClientHandler = new HttpClientHandler();
            httpClientHandler.AutomaticDecompression <- (DecompressionMethods.Deflate ||| DecompressionMethods.GZip ||| DecompressionMethods.Brotli)
            httpClientHandler.AllowAutoRedirect <- true
            httpClientHandler.MaxAutomaticRedirections <- 20
            httpClientHandler.MaxConnectionsPerServer <- 20
            httpClientHandler.UseProxy <- false
            httpClientHandler.Proxy <- null;

            // Return `true` to allow certificates that are untrusted/invalid
            httpClientHandler.ServerCertificateCustomValidationCallback <-
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator

            use cts = new CancellationTokenSource(new TimeSpan(0, 0, 80))
            use httpClient = new System.Net.Http.HttpClient(httpClientHandler)

            use! response = httpClient.GetAsync(url, cts.Token) |> Async.AwaitTask

            // We want to handle redirects ourselves so that we can determine the final redirect Location (via header)
            let statusCode = response.StatusCode |> int
            let mutable redirectUri:Uri = null
            let location = if response.Headers.Location <> null then response.Headers.Location.ToString() else String.Empty
            if (statusCode >= 300 && statusCode <= 399) 
                && not(String.IsNullOrEmpty(location)) && location <> url 
                && ((url.Length <= 80 && location.Length <= 80) || (url.Length > 80) ) 
                && not(location.StartsWith("://") && not(location.EndsWith("404.html")) ) then
                redirectUri <- response.Headers.Location
                redirectUri <- if not(redirectUri.IsAbsoluteUri) then new Uri(new Uri((new Uri(url)).GetLeftPart(UriPartial.Authority)), redirectUri) else redirectUri 
                printfn "getHtmlAsStringAsync(url:string) Redirecting to %s" (redirectUri.ToString())
                if linkRedirection.Add(redirectUri.ToString()) then
                    let! content = getHtmlAsync(redirectUri.ToString())
                    return content
                else
                    linkRedirection.Clear()
                    return raise <| System.ArgumentException("Cycled redirect location detected!")
            else 
                response.EnsureSuccessStatusCode () |> ignore
                let! content = response.Content.ReadAsStringAsync() |> Async.AwaitTask
                return content
        }
    getHtmlAsync(url)

let asyncEmailExtractor(url:string, companyId: int, visitedExtractor:ConcurrentCollections.ConcurrentHashSet<string>, notaddtolist:bool)=
    async { 
         try
            if not(String.IsNullOrEmpty(url)) && isNotImageExt(url) && visitedExtractor.Add(url)  then 
                printfn "asyncEmailExtractor CompanyId - %d. Url : %s" companyId url

                let! html = getHtmlAsStringAsync(url)

                let emails = MultipleEmailRegex(System.Text.RegularExpressions.RegexOptions.Compiled, regExpSpanTimeOut).Matches(html)
                             |> Seq.map (fun x -> x.Value)
                             |> List.ofSeq |> Seq.distinct
                             |> Seq.filter (fun eml -> eml.EndsWith("Mail.ru") |> not)

                if not(notaddtolist) && ((Seq.isEmpty emails) |> not) then
                   emails |> Seq.iter (fun x -> printfn "Email - %s" x
                                                companyEmailBag.Add({ CompanyId = companyId; Email=x }))

                return emails;
            else
                return Seq.empty
         with
         | :? System.Net.WebException as ex ->
                 printfn "asyncEmailExtractor url - %s error - %s" url (trunc(ex.Message))
                 return Seq.empty
         | _ as ex -> printfn "asyncEmailExtractor url - %s error - %s" url (trunc(ex.Message))
                      return Seq.empty
        }

    
    
let emailExtractor(url:string, companyId: int, visitedExtractor:ConcurrentCollections.ConcurrentHashSet<string>)=
    asyncEmailExtractor(url, companyId, visitedExtractor, false)
    |> Async.RunSynchronously

let emailGraber (url:string, html:string, companyId: int, visitedGraber:ConcurrentCollections.ConcurrentHashSet<string>, visitedExtractor:ConcurrentCollections.ConcurrentHashSet<string>) = 
    let baseUrl = getBaseUrl(url) 
    let links = searchHrefResults(html, baseUrl)
    if visitedGraber.Add(url) then
        emailExtractor(url, companyId, visitedExtractor) |> ignore

    (links |> Seq.chunkBySize 50) 
    |> Seq.iter (fun xurl -> let compsData = Async.Parallel [for purl in xurl -> asyncEmailExtractor(purl, companyId, visitedExtractor, true) ] |> Async.RunSynchronously
                             compsData |> Array.iter(fun eml -> eml |> Seq.iter(fun e ->printfn "Email - %s" e
                                                                                        companyEmailBag.Add({ CompanyId = companyId; Email=e })
                                                                                )
                                                     )
                             xurl |> Array.iter(fun curl -> visitedGraber.Add(curl) |> ignore)
                )

/// Crawl the internet starting from the specified page.
/// From each page follow the first not-yet-visited page.
let rec randomCrawl url companyId visitedEmailExtractor = 
  let visited = new ConcurrentCollections.ConcurrentHashSet<_>()
  let visitedEmailGraber = new ConcurrentCollections.ConcurrentHashSet<string>()
  // Visits page and then recursively visits all referenced pages
  let rec loop url = async {
    if visited.Add(url) then
        try
          let baseUrl = getBaseUrl(url)
          let! htmlDoc = HtmlDocument.AsyncLoad(url)
          let htmlData = htmlDoc.ToString()
          let html = if not (String.IsNullOrEmpty(htmlData)) then
                          htmlData
                     else  
                          getHtmlAsStringAsync(url) |> Async.RunSynchronously
          printfn "randomCrawl CompanyId - %d. Url : %s" companyId url
          emailGraber(url, html, companyId, visitedEmailGraber, visitedEmailExtractor )
          for link in searchHrefResults(html, baseUrl) do
              do! loop link  
        with
        | :? System.Net.WebException as ex ->
                printfn "crawler loop url -%s error - %s" url ex.Message
        | _ as ex -> printfn "crawler loop url -%s error - %s" url ex.Message
    }
  loop url

let visitedEmailCrawler = new ConcurrentCollections.ConcurrentHashSet<string>()

let asyncEmailCrawler (url:string, companyId: int) = 
    async{
        try
            if not(String.IsNullOrWhiteSpace(url)) && visitedEmailCrawler.Add(url) then
                let visitedEmailExtractor = new ConcurrentCollections.ConcurrentHashSet<string>()
                printfn "asyncEmailCrawler CompanyId - %d. Url : %s" companyId url
                let baseUrl = getBaseUrl(url)

                let! html = getHtmlAsStringAsync(url)
                  
                if not (String.IsNullOrWhiteSpace(html)) then

                    let mailto = searchMailToResults(html) 

                    if not(Seq.isEmpty mailto) then
                       mailto |> Seq.iter(fun e -> printfn "Email - %s" e
                                                   companyEmailBag.Add({ CompanyId = companyId; Email=e }) )

                    let emails = MultipleEmailRegex(System.Text.RegularExpressions.RegexOptions.Compiled, regExpSpanTimeOut).Matches(html)
                                 |> Seq.map (fun x -> x.Value)
                                 |> List.ofSeq |> Seq.distinct
                                 |> Seq.filter (fun eml -> eml.EndsWith("Mail.ru") |> not)

                    if not(Seq.isEmpty emails) then
                        emails |> Seq.iter(fun e -> printfn "Email - %s" e
                                                    companyEmailBag.Add({ CompanyId = companyId; Email=e }) )

                    if not(Seq.isEmpty mailto) && (Seq.length mailto) > 1 then
                        //updateCompanyEmailProcessed(companyId, true)
                        updateCompanyFlags (companyId, false, true, true)
                    else
                        let linksSearch = FindLinksRegExp(html, baseUrl)

                        let dataLink = linksSearch
                                       |> Seq.filter (fun (name, _) -> name.ToLower().Contains("контакты") 
                                                                       || name.ToLower().Contains("contacts") 
                                                                       || name.ToLower().Contains("обратная связь")
                                                                       || name.ToLower().Contains("feedback") )
                                       |> Seq.map (fun (_, url) -> url)

                        dataLink |> Seq.iter (fun l -> (asyncEmailExtractor(l, companyId, visitedEmailExtractor, false) |> Async.RunSynchronously) |> ignore)

                        let linkHrefs = if (Seq.isEmpty dataLink) then searchHrefResults(html, baseUrl) else Seq.empty
                        let data = (linkHrefs |> Seq.filter (fun url -> isContactsPage(url)) )
                   
                        (data |> Seq.chunkBySize 50) 
                              |> Seq.iter (fun x -> let compsData = Async.Parallel [for purl in x -> asyncEmailExtractor(purl, companyId, visitedEmailExtractor, true) ] |> Async.RunSynchronously 
                                                    compsData |> Array.iter(fun eml -> eml |> Seq.iter(fun e ->printfn "Email - %s" e
                                                                                                               companyEmailBag.Add({ CompanyId = companyId; Email=e }))
                                                 
                                                                           )
                                          )

                        if ( not(Seq.isEmpty dataLink) || (Seq.length data) <> 0 || not(Seq.isEmpty mailto) || not(Seq.isEmpty emails) ) then
                             updateCompanyBadUrl (companyId, false, true, true)
                             updateCompanyFlags (companyId, false, true, true)
                        else
                            updateCompanyBadUrl (companyId, false, true, false)
                            updateCompanyFlags (companyId, false, true, false)

                        //if (Seq.isEmpty dataLink) && (Seq.isEmpty data) && (Seq.isEmpty mailto) && (Seq.isEmpty emails) then
                        //    randomCrawl url companyId visitedEmailExtractor |> Async.RunSynchronously *)
                else
                    printfn "asyncEmailCrawler bad empty url. CompanyId - %d" companyId
                    updateCompanyBadUrl(companyId, true, true, false)
                    updateCompanyFlags(companyId, true, true, false)
            else
                updateCompanyFlags(companyId, true, true, false)
        with
        | :? System.Net.WebException as ex ->
                printfn "asyncEmailCrawler w url - %s error - %s" url  (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
                updateCompanyFlags (companyId, true, true, false)
        | :? HttpRequestException as ex -> 
                printfn "asyncEmailCrawler h url - %s error - %s" url (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
                updateCompanyFlags (companyId, true, true, false)
        | :? System.Net.CookieException as ex ->
                printfn "asyncEmailCrawler c url - %s error - %s" url (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
                updateCompanyFlags (companyId, true, true, false)
        | _ as ex -> printfn "asyncEmailCrawler g url - %s error - %s" url (trunc(ex.Message))
                     updateCompanyBadUrl (companyId, true, true, false)
                     updateCompanyFlags (companyId, true, true, false)
    }

let emailCrawler (url:string, companyId: int) = 
    asyncEmailCrawler(url, companyId) |> Async.RunSynchronously

