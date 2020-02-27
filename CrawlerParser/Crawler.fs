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

let isNotImageExt(str:string) = 
    (not(str.ToLower().EndsWith(".png")) 
    && not(str.ToLower().EndsWith(".jpg")) 
    && not(str.ToLower().EndsWith(".jpeg")) 
    && not(str.ToLower().EndsWith(".jfif")) 
    && not(str.ToLower().EndsWith(".bmp")) 
    && not(str.ToLower().EndsWith(".gif")) 
    && not(str.ToLower().EndsWith(".tiff"))
    && not(str.ToLower().EndsWith(".tif"))
    && not(str.ToLower().EndsWith(".webp"))
    && not(str.ToLower().EndsWith(".js"))
    && not(str.ToLower().EndsWith(".css")))

let notSocialNetwork(x:string) =
    not(x.Contains("facebook.com"))
    && not(x.Contains("twitter.com"))
    && not(x.Contains("pinterest.com"))
    && not(x.Contains("linkedin.com"))
    && not(x.Contains("instagram.com"))
    && not(x.Contains("youtube.com"))
    && not(x.Contains("vk.com"))

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

let contactPage(str:string) =
    let str = str.ToLower()
    (str.Contains("contact") 
     || str.Contains("kontakt")
     || str.Contains("contac")
     || str.Contains("контакт")
     || str.Contains("contact-us") 
     || str.Contains("contact-me") 
     || str.Contains("about")
     || str.Contains("about-us")
     || str.Contains("about-me")
     || str.Contains("home")
     || str.Contains("feedback")
     || str.Contains("media")
     || str.Contains("event")
     || str.EndsWith("kont") || str.EndsWith("kont#")
     || str.EndsWith("cont")
     || str.EndsWith("addresses")
     || str.EndsWith("address")
     || (str.EndsWith("map") && not(str.Contains("sitemap")))
     || (str.Contains("page") && str.EndsWith(@".html") && (str.ToCharArray().[str.Length-6] |> Char.IsDigit) )
     || str.EndsWith("info") 
     || str.EndsWith("mail") 
     || str.Contains("ofis")
     || str.EndsWith("ofis_centrov") || str.EndsWith("ofis_tverskaja")
     || str.EndsWith("shop_content.php?coID=12")
     || str.Contains("o-magazine")
     || str.Contains("sendmail")
     || str.Contains("page_id")
     || (str.Contains("index.php?") && not(str.Contains("category")) && not(str.Contains("product")))
     || (str.Contains("index.php?") && not(str.Contains("catalog")) && not(str.Contains("SECTION")))
     || (str.Contains("index.php?") && str.Contains("show") && not(str.Contains("page")))
     )

let isContactsPage(x:string)=
    let contactPages = (x.Contains("contact") || x.Contains("kontakt"))
    let str = x.TrimEnd(@"/".ToCharArray())
    let lastIndex = str.LastIndexOf @"/"
    let length = str.Length - lastIndex - 1
    if lastIndex > 0 && length > 0 then
       let substr = str.Substring(lastIndex + 1, length)
       contactPage(substr) || contactPages
    elif str.Length > 4 then
        contactPage(str) || contactPages
    else
       false || contactPages

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

    EmailRegex().Matches(html)
    |> Seq.map (fun x -> x.Value.TrimStart("href=".ToCharArray()).Trim("\"".ToCharArray()).TrimEnd("\">".ToCharArray()) )
    |> Seq.filter (fun x -> x.Length >= 5)
    |> Seq.filter (fun x -> not(x.ToLower().StartsWith("mailto:")))
    |> Seq.filter (fun x -> (not(x.Contains("?")) && not(x.Contains("%")) 
                                && not(x.Contains("~")) && not(x.Contains("catalog")) && not(x.Contains("category")) 
                                && not(x.Contains("topic")) && not(x.Contains("template")) && not(x.Contains("brend")) 
                                && not(x.Contains("node")) && not(x.Contains("goods")) && not(x.Contains("drivers"))
                                && not(x.Contains("help")) && not(x.Contains("news")) 
                                && not(x.Contains("product")) && not(x.Contains("promo")) 
                                && not(x.Contains("sites")) && not(x.Contains("service")) && not(x.Contains("/info/")) 
                                && not(x.Contains("novosti")) && not(x.Contains("konferenc")) && not(x.ToLower().Contains("archive"))
                                && not(x.Contains("apartments")) && not(x.Contains("blog")) 
                                && not(x.Contains("print")) && not(x.Contains("center"))
                                && not(x.Contains("subscription")) && not(x.Contains("province"))
                                && not(x.Contains("item")) && not(x.Contains("feed")) && not(x.Contains("tag"))
                                && not(x.Contains("desc")) && not(x.Contains("search")) && not(x.Contains("shop"))
                                && not(x.Contains("courses")) && not(x.Contains("snap")) && not(x.Contains("albums"))
                                && not(x.Contains("show")) && not(x.Contains("photo")) && not(x.Contains("presentation"))
                                && not(x.Contains("clients")) && not(x.Contains("articles")) && not(x.Contains("business"))
                                && not(x.Contains("project")) && not(x.Contains("dokument")) && not(x.Contains("image"))
                                && not(x.Contains("proekt"))  && not(x.Contains("formy")) && not(x.Contains("kursy"))
                                && not(x.Contains("lists"))  && not(x.Contains("biz")) && not(x.Contains("directory"))
                                && not(x.Contains("places")) && not(x.Contains("api")) && not(x.Contains("katalog"))
                                && not(x.Contains("index")) && not(x.Contains("bank")) && not(x.Contains("press"))
                                && not(x.Contains("css")) && not(x.Contains("donate")) && not(x.Contains("respond"))
                                && not(x.Contains("cat")) && not(x.Contains("online"))  && not(x.Contains("page"))
                                && not(x.Contains("comment")) && not(x.Contains("stil")) && not(x.Contains("design"))
                                && not(x.Contains("privacy")) && not(x.Contains("watch")) && not(x.Contains("store"))
                                && not(x.Contains("addresses")) && not(x.Contains("before")) && not(x.Contains("after"))
                                && not(x.Contains("brands")) && not(x.Contains("rieltor")) && not(x.Contains("onlayn"))
                                && not(x.Contains("cars")) && not(x.Contains("options")) && not(x.Contains("results"))
                                && not(x.Contains("video")) && not(x.Contains("conditions")) && not(x.Contains("?url"))
                                && not(x.Contains("porn")) && not(x.Contains("teacher")) && not(x.Contains("card")) 
                                && not(x.Contains("letters")) && not(x.Contains("obrazovanie")) && not(x.Contains("arenda"))
                                && not(x.ToLower().Contains("estates")) && not(x.ToLower().EndsWith(".js"))
                                && not(x.Contains("ajax"))) || (isNotImageExt(x) && isContactsPage(x)) 
                                || (x.Contains("?") && x.Contains("topmenu")) )
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

let headers = [ 
                UserAgent "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 70.0.3538.77 Safari / 537.36"
                Accept "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" 
                KeepAlive "true"
              ]

let customized (req : HttpWebRequest) = 
       req.AllowAutoRedirect <- false
       req.Timeout <- 4000
       req

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
                          Http.RequestString(url, headers = headers, customizeHttpRequest = customized)
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

