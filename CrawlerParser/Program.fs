// Learn more about F# at http://fsharp.org

open System
open FSharp.Data
open FSharp.Data.Sql
open System.Collections.Generic
open System.Net
open System.Diagnostics
open System.Text
open Utils
open DomainModel
open DbAccess
open HtmlParsing
open Crawler


[<EntryPoint>]
let main _ =
    System.AppDomain.CurrentDomain.UnhandledException.AddHandler(
        fun _ (args:System.UnhandledExceptionEventArgs) ->
           lock monitor (fun () ->
                 printfn "UnhandledException %s" <| (args.ExceptionObject :?> System.Exception).ToString()
              )
    )

    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance)
    ServicePointManager.Expect100Continue <- false
    ServicePointManager.SecurityProtocol <- SecurityProtocolType.Tls13 ||| SecurityProtocolType.Tls12 ||| SecurityProtocolType.Tls11 ||| SecurityProtocolType.Tls

    // Moscow
    let cityCodeMoscow = "77"
    let regionHtmlDoc = HtmlDocument.Load("http://www.statdata.ru/spisok-regionov-rossii-s-kodamy")
    let regionList = getRussionRegionList(regionHtmlDoc) 
    insertBulkRegionData regionList

    let cityHtmlDoc = HtmlDocument.Load("https://hramy.ru/regions/city_reg.htm", Encoding.GetEncoding("Windows-1251"))
    let cityList = getRussionCitiesList(cityHtmlDoc)
    insertBulkCityData cityList

    let html = HtmlDocument.Load("https://ru.freeflagicons.com/list/")
    let countries = getCountryList html
    insertCountryData countries

    let baseUrl = "https://moskva.bizly.ru"
    let productHtml = HtmlDocument.Load(baseUrl)

    let resultList = new List<TreeNode>()
    getDivRow (productHtml, resultList)
    
    // load header category
    List.ofSeq resultList 
    |> Seq.filter (fun x -> x.ParentId = 0)
    |> Seq.iter (fun x -> insertCategoryData (x.Name, -1, null, x.CityCode) ) 

    // load sub header category
    List.ofSeq resultList 
    |> Seq.filter (fun x -> x.Id >= 1000 && x.Id <= 100000)
    |> Seq.iter (fun x -> let item = List.ofSeq resultList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                          let id = (getCategoryId (item.Name, cityCodeMoscow)).IndustryCategoryId
                          insertCategoryData (x.Name, id, null, x.CityCode) )


    // load category with url data
    List.ofSeq resultList 
    |> Seq.filter (fun x -> x.Href <> null)
    |> Seq.iter (fun x -> let item = List.ofSeq resultList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                          let id = (getCategoryId (item.Name, cityCodeMoscow)).IndustryCategoryId
                          insertCategoryData (x.Name, id, (sprintf "%s%s" baseUrl x.Href), x.CityCode))

    let resultCompany = new List<Company>()

    // load company info from pages Moscow
    printfn "Category Number - %d" ((List.ofSeq resultList |> Seq.filter (fun x -> x.Href <> null)) |> Seq.length)
    List.ofSeq resultList 
    |> Seq.filter (fun x -> x.Href <> null)
    |> Seq.iter (fun x ->  let category = (getCategoryId (x.Name, cityCodeMoscow))
                           let id = category.IndustryCategoryId
                           if not category.CompanyProcessed then
                               let mutable pnum:int=0
                               let currentUrl = (sprintf "%s%s" baseUrl x.Href)
                               try
                                   let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                   retry {
                                       let companyHtml = HtmlDocument.Load(currentUrl)
                                       pnum <- getPageNumber(companyHtml, "ul") 
                                       printfn "currentUrl - %s, page number - %d" currentUrl pnum
                                       getDivCompany (companyHtml, resultCompany, id) 
                                   }
                               with
                                   | Failure(msg) -> printfn "url - %s error:  %s" currentUrl msg;

                               let mutable curPnum:int = 2
                               while curPnum <= pnum do
                                  let curl = (sprintf "%s%spage-%d/" baseUrl x.Href curPnum)
                                  try
                                      let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                      retry {
                                          let companyHtml = HtmlDocument.Load(curl)
                                          getDivCompany (companyHtml, resultCompany, id) 
                                          curPnum<-curPnum+1
                                      }
                                  with
                                      | Failure(msg) -> printfn "url - %s error:  %s" curl msg
                                                        curPnum<-curPnum+1
                               updateCategoryCompanyProcessed (id, true)
                )
    try
        insertBulkCompanyData(resultCompany)
    with 
        | _ as ex -> printfn "error:  %s" ex.Message
   
    // Sankt-Peterburg
    let cityCodeSanktPeterburg = "78"
    let baseUrlCityCode78 = "https://spb.spravker.ru"
    let tDoc = HtmlDocument.Load(baseUrlCityCode78)
    let treeNodeList = getCategoryLinks (tDoc, baseUrlCityCode78, cityCodeSanktPeterburg)

    // load header category for citycode 78 (Sankt-Peterburg)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.ParentId = 0)
    |> Seq.iter (fun x -> insertCategoryData (x.Name, -1, x.Href, x.CityCode) )
    
    // load category with url data for citycode 78 (Sankt-Peterburg)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x -> let item = List.ofSeq treeNodeList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                          let id = (getCategoryId (item.Name, cityCodeSanktPeterburg)).IndustryCategoryId
                          insertCategoryData (x.Name, id, (sprintf "%s%s" baseUrlCityCode78 x.Href), x.CityCode))

    let sws = Stopwatch()
    sws.Start()

    let compList = List<Company>()
    let baseUrlSanktPeterburg = "https://spb.spravker.ru"
    // load company info from pages citycode 78 (Sankt-Peterburg)
    printfn "Category Number - %d" ((List.ofSeq treeNodeList |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )) |> Seq.length)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x ->  let category = (getCategoryId (x.Name, cityCodeSanktPeterburg))
                           let id = category.IndustryCategoryId
                           if not category.CompanyProcessed then
                               let mutable pnum:int=0
                               let currentUrl = (sprintf "%s%s" baseUrlSanktPeterburg x.Href)
                               try
                                   let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                   retry {
                                       let companyHtml = HtmlDocument.Load(currentUrl)
                                       pnum <- getPageNumber(companyHtml, "div") 
                                       printfn "currentUrl - %s, page number - %d" currentUrl pnum
                                       getDivCompanies (companyHtml, compList, id) 
                                   }
                               with
                                   | Failure(msg) -> printfn "url - %s error:  %s" currentUrl msg;

                               let curls = seq { for i in 2 .. pnum -> (sprintf "%s%spage-%d/" baseUrlSanktPeterburg x.Href i) }
                               let getCompanyAsyncData url =
                                        async { 
                                                let list = List<Company>()
                                                try
                                                    let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                                    retry {
                                                        async {
                                                            let! companyHtml = HtmlDocument.AsyncLoad(url)
                                                            getDivCompanies (companyHtml, list, id)
                                                        } |> Async.RunSynchronously
                                                    }
                                                with
                                                    | Failure(msg) -> printfn "url - %s error:  %s" url msg; 

                                                return list }
                               curls |> Seq.chunkBySize 20 
                               |> Seq.iter (fun x -> let compsData = Async.Parallel [for purl in x -> getCompanyAsyncData purl ] |> Async.RunSynchronously
                                                     compsData |> Array.iter(fun ar-> compList.AddRange(ar))
                                                     )
                               updateCategoryCompanyProcessed (id, true)
                )
    sws.Stop()
    printfn "Load company info from pages for CityCode - %s Time - %s" cityCodeSanktPeterburg (sws.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
    try
        let swes = Stopwatch()
        swes.Start()

        printfn "Company Count %d for CityCode - %s" compList.Count cityCodeSanktPeterburg
        insertBulkCompanyData(compList)

        swes.Stop()
        printfn "Save company info for CityCode - %s Time - %s" cityCodeSanktPeterburg (swes.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
    with 
        | _ as ex -> printfn "error:  %s" ex.Message 

    // Novosibirsk
    let cityCodeNovosibirsk = "54"
    let baseUrlCityCode54 = "https://novosibirsk.jsprav.ru"
    let doc = HtmlDocument.Load(baseUrlCityCode54)
    let treeNodeList = getNovCategoryLinks (doc, baseUrlCityCode54, cityCodeNovosibirsk)

    // load header category for citycode 54 (Novosibirsk)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.ParentId = 0)
    |> Seq.iter (fun x -> insertCategoryData (x.Name, -1, x.Href, x.CityCode) )
    
    // load category with url data for citycode 54 (Novosibirsk)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x -> let item = List.ofSeq treeNodeList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                          let id = (getCategoryId (item.Name, cityCodeNovosibirsk)).IndustryCategoryId
                          insertCategoryData (x.Name, id, (sprintf "%s%s" baseUrlCityCode54 x.Href), x.CityCode))

    
    let swn = Stopwatch()
    swn.Start()

    // load company info from pages citycode 54 (Novosibirsk)
    printfn "Novosibirsk Category Number - %d" ((List.ofSeq treeNodeList |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )) |> Seq.length)
    let compList = List<Company>()
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x ->  let category = (getCategoryId (x.Name, cityCodeNovosibirsk))
                           let id = category.IndustryCategoryId
                           if not category.CompanyProcessed then
                               let mutable pnum:int=0
                               let currentUrl = (sprintf "%s%s" baseUrlCityCode54 x.Href)
                               try
                                   let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                   retry {
                                       let companyHtml = HtmlDocument.Load(currentUrl)
                                       pnum <- getPageNumber(companyHtml, "ul") 
                                       printfn "currentUrl - %s, page number - %d" currentUrl pnum
                                       getNovDivCompanies (companyHtml, compList, id) 
                                   }
                                   System.Threading.Thread.Sleep(1000);
                               with
                                   | Failure(msg) -> printfn "url - %s error:  %s" currentUrl msg;

                               let mutable curPnum:int = 2
                               while curPnum <= pnum do
                                  let curl = (sprintf "%s%s?p-%d" baseUrlCityCode54 x.Href curPnum)
                                  try
                                      let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                      retry {
                                          let companyHtml = HtmlDocument.Load(curl)
                                          getNovDivCompanies (companyHtml, compList, id) 
                                          printfn "currentUrl - %s, current page number - %d" curl curPnum
                                          curPnum<-curPnum+1
                                      }
                                      System.Threading.Thread.Sleep(3000);
                                  with
                                      | Failure(msg) -> printfn "url - %s error:  %s" curl msg
                                                        curPnum<-curPnum+1
                               updateCategoryCompanyProcessed (id, true)
                )
    swn.Stop()
    printfn "Load company info from pages citycode 54(Novosibirsk) Time - %s" (swn.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))

    try
        let swes = Stopwatch()
        swes.Start()

        printfn "Company Count %d for CityCode - %s" compList.Count cityCodeNovosibirsk
        insertBulkCompanyData(compList)

        swes.Stop()
        printfn "Save company info for CityCode - %s Time - %s" cityCodeNovosibirsk (swes.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
    with 
        | _ as ex -> printfn "error:  %s" ex.Message

    // Novgorod
    (*let cityCodeNovgorod = "52"
    let baseUrlCityCode52 = "https://nizhnij-novgorod.jsprav.ru"
    let doc = HtmlDocument.Load(baseUrlCityCode52)
    let treeNodeList = getNovCategoryLinks (doc, baseUrlCityCode52, cityCodeNovgorod)

    // load header category for citycode 52 (Novgorod)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.ParentId = 0)
    |> Seq.iter (fun x -> insertCategoryData (x.Name, -1, x.Href, x.CityCode) )
    
    // load category with url data for citycode 52 (Novgorod)
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x -> let item = List.ofSeq treeNodeList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                          let id = (getCategoryId (item.Name, cityCodeNovgorod)).IndustryCategoryId
                          insertCategoryData (x.Name, id, (sprintf "%s%s" baseUrlCityCode52 x.Href), x.CityCode))

    
    let swn = Stopwatch()
    swn.Start()

    // load company info from pages citycode 54 (Novgorod)
    printfn "Novgorod Category Number - %d" ((List.ofSeq treeNodeList |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )) |> Seq.length)
    let compList = List<Company>()
    List.ofSeq treeNodeList 
    |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
    |> Seq.iter (fun x ->  let category = (getCategoryId (x.Name, cityCodeNovgorod))
                           let id = category.IndustryCategoryId
                           if not category.CompanyProcessed then
                               let mutable pnum:int=0
                               let currentUrl = (sprintf "%s%s" baseUrlCityCode52 x.Href)
                               try
                                   let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                   retry {
                                       let companyHtml = HtmlDocument.Load(currentUrl)
                                       pnum <- getPageNumber(companyHtml, "ul") 
                                       printfn "currentUrl - %s, page number - %d" currentUrl pnum
                                       getNovDivCompanies (companyHtml, compList, id) 
                                   }
                                   System.Threading.Thread.Sleep(1000);
                               with
                                   | Failure(msg) -> printfn "url - %s error:  %s" currentUrl msg;

                               let mutable curPnum:int = 2
                               while curPnum <= pnum do
                                  let curl = (sprintf "%s%s?p-%d" baseUrlCityCode52 x.Href curPnum)
                                  try
                                      let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                      retry {
                                          let companyHtml = HtmlDocument.Load(curl)
                                          getNovDivCompanies (companyHtml, compList, id) 
                                          printfn "currentUrl - %s, current page number - %d" curl curPnum
                                          curPnum<-curPnum+1
                                      }
                                      System.Threading.Thread.Sleep(3000);
                                  with
                                      | Failure(msg) -> printfn "url - %s error:  %s" curl msg
                                                        curPnum<-curPnum+1

                               updateCategoryCompanyProcessed (id, true)
                )
    swn.Stop()
    printfn "Load company info from pages citycode 52(Novgorod) Time - %s" (swn.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))

    try
        let swes = Stopwatch()
        swes.Start()

        printfn "Company Count %d for CityCode - %s" compList.Count cityCodeNovgorod
        insertBulkCompanyData(compList)

        swes.Stop()
        printfn "Save company info for CityCode - %s Time - %s" cityCodeNovgorod (swes.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
    with 
        | _ as ex -> printfn "error:  %s" ex.Message
    *) 

    let listLinks =[
                     ("https://ekaterinburg.jsprav.ru","66");
                     ("https://kazan.jsprav.ru","16");
                     ("https://omsk.jsprav.ru","55");
                     ("https://chelyabinsk.jsprav.ru","74");
                     ("https://samara.jsprav.ru","55");
                     ("https://ufa.jsprav.ru","02");
                     ("https://krasnoyarsk.jsprav.ru","24");
                     ("https://voronezh.jsprav.ru","36");
                     ("https://volgograd.jsprav.ru","59");
                     ("https://krasnodar.jsprav.ru","23");
                     ("https://saratov.jsprav.ru","64");
                     ("https://tyumen.jsprav.ru","72");
                     ("https://tolyatti.jsprav.ru","63");
                     ("https://izhevsk.jsprav.ru","18");
                     ("https://barnaul.jsprav.ru","22");
                     ("https://ulyanovsk.jsprav.ru","73");
                     ("https://irkutsk.jsprav.ru","38");
                     ("https://habarovsk.jsprav.ru","27");
                     ("https://yaroslavl.jsprav.ru","76");
                     ("https://vladivostok.jsprav.ru","25");
                     ("https://mahachkala.jsprav.ru","05");
                     ("https://tomsk.jsprav.ru","70");
                     ("https://orenburg.jsprav.ru","56");
                     ("https://kemerovo.jsprav.ru","42");
                     ("https://ryazan.jsprav.ru","62");
                     ("https://astrahan.jsprav.ru","30");
                     ("https://penza.jsprav.ru","58");
                     ("https://kirov.jsprav.ru","43");
                     ("https://lipetsk.jsprav.ru","48");
                     ("https://balashiha.jsprav.ru","50");
                     ("https://kaliningrad.jsprav.ru","39");
                     ("https://tula.jsprav.ru","71");
                     ("https://kursk.jsprav.ru","46");
                     ("https://sochi.jsprav.ru","23");
                     ("https://stavropol.jsprav.ru","26");
                     ("https://ulan-ude.jsprav.ru","03");
                     ("https://tver.jsprav.ru","69");
                     ("https://magnitogorsk.jsprav.ru","74");
                     ("https://ivanovo.jsprav.ru","37");
                     ("https://kamensk-uralskij.jsprav.ru", "66");
                     ("https://blagoveschensk.jsprav.ru", "28");
                    ]
    let resultlistLinks = MutableList<string * string>.empty
    let resultProblemCityNames = MutableList<string>.empty
    let doc = HtmlDocument.Load("http://www.statdata.ru/largest_cities_russia")
    let resultCities = getCities(doc) |> Seq.where(fun x -> x.Code="6")
    let resCities = resultCities |> Seq.skip(1) |> Seq.take ((Seq.length resultCities) - 2) 
    resCities 
    |> Seq.iter (fun x -> let mutable citytName:string=""
                          if x.Name = "Артем" then
                             citytName <- "Артём"
                          else
                             citytName <- x.Name
                          let latinCity = translitCyrillicToLatin(x.Name)
                          let url  = sprintf "https://%s.jsprav.ru/" latinCity
                          try
                             let cityCode = getCity(x.Name)
                             let _ = HtmlDocument.Load(url) 
                             resultlistLinks.Add((url.ToLower(),cityCode.Code)) |> ignore
                          with 
                              | _ -> resultProblemCityNames.Add(x.Name) |> ignore
    )

    listLinks@resultlistLinks.Value 
    |> List.ofSeq
    |> Seq.iter(fun (link, code)->
        if not(checkCategoryCityCode(code)) then
            // Novgorod
            let cityCode  = code
            let baseUrlByCityCode = link
            let doc = HtmlDocument.Load(baseUrlByCityCode)
            let treeNodeList = getNovCategoryLinks (doc, baseUrlByCityCode, cityCode)

            // load header category for current citycode 
            List.ofSeq treeNodeList 
            |> Seq.filter (fun x -> x.ParentId = 0)
            |> Seq.iter (fun x -> insertCategoryData (x.Name, -1, x.Href, x.CityCode) )
        
            // load category with url data for current citycode 
            List.ofSeq treeNodeList 
            |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
            |> Seq.iter (fun x -> let item = List.ofSeq treeNodeList |> Seq.filter (fun m -> m.Id = x.ParentId) |>  Seq.exactlyOne
                                  let id = (getCategoryId (item.Name, cityCode)).IndustryCategoryId
                                  insertCategoryData (x.Name, id, (sprintf "%s%s" baseUrlByCityCode x.Href), x.CityCode))

        
            let swn = Stopwatch()
            swn.Start()

            // load company info from pages for current citycode 
            printfn "Current Category Number - %d" ((List.ofSeq treeNodeList |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )) |> Seq.length)
            let compList = List<Company>()
            List.ofSeq treeNodeList 
            |> Seq.filter (fun x -> x.Id >= 100000 && x.ParentId > 0 )
            |> Seq.iter (fun x ->  let category = (getCategoryId (x.Name, cityCode))
                                   let id = category.IndustryCategoryId
                                   if not category.CompanyProcessed then
                                       let mutable pnum:int=0
                                       let currentUrl = (sprintf "%s%s" (baseUrlByCityCode.TrimEnd("/".ToCharArray())) (x.Href.Replace("//","/")))
                                       try
                                           let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                           retry {
                                               let companyHtml = HtmlDocument.Load(currentUrl)
                                               pnum <- getPageNumber(companyHtml, "ul") 
                                               printfn "currentUrl - %s, page number - %d" currentUrl pnum
                                               getNovDivCompanies (companyHtml, compList, id) 
                                           }
                                           System.Threading.Thread.Sleep(1000);
                                       with
                                           | Failure(msg) -> printfn "url - %s error:  %s" currentUrl msg;

                                       let mutable curPnum:int = 2
                                       while curPnum <= pnum do
                                          let curl = (sprintf "%s%s?p-%d" (baseUrlByCityCode.TrimEnd("/".ToCharArray())) (x.Href.Replace("//","/")) curPnum)
                                          try
                                              let retry = RetryBuilder(3, TimeSpan.FromSeconds(1.))
                                              retry {
                                                  let companyHtml = HtmlDocument.Load(curl)
                                                  getNovDivCompanies (companyHtml, compList, id) 
                                                  printfn "currentUrl - %s, current page number - %d" curl curPnum
                                                  curPnum<-curPnum+1
                                              }
                                              System.Threading.Thread.Sleep(2000);
                                          with
                                              | Failure(msg) -> printfn "url - %s error:  %s" curl msg
                                                                curPnum<-curPnum+1

                                       updateCategoryCompanyProcessed (id, true)
                        )
            swn.Stop()
            printfn "Load company info from pages citycode %s Time - %s" cityCode (swn.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))

            try
                let swes = Stopwatch()
                swes.Start()

                printfn "Company Count %d for CityCode - %s" compList.Count cityCode
                insertBulkCompanyData(compList)

                swes.Stop()
                printfn "Save company info for CityCode - %s Time - %s" cityCode (swes.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
            with 
                | _ as ex -> printfn "error:  %s" ex.Message
    
    )
           
    printfn "Email crawler start. Companies Site Urls - %d" (getCompanies 
                                                            |> Seq.filter (fun x -> x.SiteUrl <> null && not(String.IsNullOrEmpty(x.SiteUrl)) && not(String.IsNullOrWhiteSpace(x.SiteUrl))) 
                                                            |> Seq.length)
    let findedEmails = ref 0

    try
        let sw = Stopwatch()
        sw.Start()

        printfn "ServicePointManager.DefaultConnectionLimit - %d" ServicePointManager.DefaultConnectionLimit
        ServicePointManager.DefaultConnectionLimit <- 100 // not used settings actual for WebRequest in .net core
        ServicePointManager.ServerCertificateValidationCallback <- System.Net.Security.RemoteCertificateValidationCallback(fun _ _ _ _ -> true) // not used Http.RequestString (WebRequest)

        let count = ref 0

        let clenght = 
            getCompanies 
            |> List.ofSeq
            |> Seq.filter (fun x -> x.SiteUrl <> null 
                                    && not(String.IsNullOrEmpty(x.SiteUrl)) 
                                    && not(String.IsNullOrWhiteSpace(x.SiteUrl)) 
                                    && not(x.BadUrl) 
                                    && not(x.EmailProcessed)    
                                    && not(x.EmailFinded)
                                    && notSocialNetwork(x.SiteUrl)
                           )
            |> Seq.distinctBy (fun x -> x.SiteUrl) 

        printfn "Companies Count for processing(filtered) - %d" (Seq.length clenght)
        if (Seq.length clenght) = 0 then
            cprintfn ConsoleColor.Green "No Companies for processing!"
            cprintfn ConsoleColor.Red "No Companies for processing. Finished!"
        else
            getCompanies |> List.ofSeq
            |> Seq.filter (fun x -> x.SiteUrl <> null 
                                    && not(String.IsNullOrEmpty(x.SiteUrl)) 
                                    && not(String.IsNullOrWhiteSpace(x.SiteUrl)) 
                                    && not(x.BadUrl) && not(x.EmailProcessed) 
                                    && not(x.EmailFinded)
                                    && notSocialNetwork(x.SiteUrl)
                                    && x.SiteUrl <> "http://www.abordageshop.ru"
                                     )
            |> Seq.distinctBy (fun x -> x.SiteUrl)   
            |> Seq.take 100000
            |> Seq.map ( fun x -> asyncEmailCrawler(x.SiteUrl, x.CompanyId) )
            |> Seq.chunkBySize 100 
            |> Seq.iter (fun x -> count := !count + 1
                                  x |> (fun task -> Async.Parallel(task)) //, ?maxDegreeOfParallelism = Some 200
                                    |> Async.RunSynchronously
                                    |> ignore 
                                
                                  let resultFiltered = 
                                        companyEmailBag.ToArray() 
                                        |> List.ofSeq 
                                        |> Seq.distinct
                                        |> Seq.filter(fun e -> isValidEmail(e.Email))
                                        |> Seq.filter(fun e -> isNotImageExt(e.Email) && not(e.Email.ToCharArray().[e.Email.Length-1] |> Char.IsDigit))
                                        |> Seq.where(fun e -> not(e.Email.ToLower().EndsWith(".html")) && not(e.Email.StartsWith("@")) )
                                        |> Seq.map(fun e -> { CompanyId = e.CompanyId; Email = trimEmailWithSubject(e.Email) } )
                                        |> Seq.filter(fun e -> e.Email.Length <= 150) 

                                  insertCompanyEmail resultFiltered
                              
                                  printfn "Email Finded - %d" (Seq.length resultFiltered)
                                  findedEmails := !findedEmails + (Seq.length resultFiltered)
                                  printfn "Count - %d END **********************************************************" !count 
                                  cprintfn ConsoleColor.Red "Count END **********************************************************"
                                  companyEmailBag.Clear()
                                    )

        sw.Stop()
        printfn "Time - %s" (sw.Elapsed.ToString(@"dd\.hh\:mm\:ss\.ff"))
    with 
        | _ as ex -> printfn "error:  %s" ex.Message 

    printfn "Finded emails - %d." (!findedEmails)
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
