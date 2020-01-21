// Learn more about F# at http://fsharp.org

open System
open FSharp.Data
open FSharp.Data.Sql
open System.Collections.Generic
open FSharp.Text.RegexProvider
open FSharp.Data.HttpRequestHeaders
open System.Net
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Net.Http
open System.Text
open System.Threading.Tasks


type Async with
    static member WithCancellation (token:CancellationToken) operation = 
        async {
            try
                let task = Async.StartAsTask (operation, cancellationToken = token)
                task.Wait ()
                return Some task.Result
            with 
                | :? TaskCanceledException -> return None
                | :? AggregateException -> return None
        }

    static member WithTimeout (timeout:int option) operation = 
        match timeout with
        | Some(time) -> 
            async {
                use tokenSource = new CancellationTokenSource (time)
                return! operation |> Async.WithCancellation tokenSource.Token
            }

        | _ -> 
            async { 
                let! res = operation
                return Some res
            }

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

type TreeNode = 
    struct  
        val mutable private id: int
        val mutable private parentId: int
        val mutable private name: string
        val mutable private href: string
        val mutable private cityCode: string
        new(id, name, parentId, href) = { id = id; name = name; parentId=parentId; href = href; cityCode = null }
        new(id, name, parentId) = { id = id; name = name; parentId=parentId; href = null ; cityCode = null }
        new(id, name) = { id = id; name = name; parentId = 0; href = null ; cityCode = null }
        new(id, name, parentId, href, cityCode) = { id = id; name = name; parentId=parentId; href = href; cityCode = cityCode }
        new(id, name, cityCode) = { id = id; name = name; parentId = 0; href = null ; cityCode = cityCode }
        member public x.Id
                   with get() = x.id
                   and set(id: int) = x.id <- id
        member public x.ParentId
                   with get() = x.parentId
                   and set(parentId: int) = x.parentId <- parentId
        member public x.Name
                   with get() = x.name
                   and set(name: string) = x.name <- name
        member public x.Href
                   with get() = x.href
                   and set(href: string) = x.href <- href
        member public x.CityCode
                   with get() = x.cityCode
                   and set(cityCode: string) = x.cityCode <- cityCode
     end

type Company = 
    struct  
        val mutable private id: int
        val mutable private categoryId: int
        val mutable private name: string
        val mutable private address: string
        val mutable private phone: string
        val mutable private timework: string
        val mutable private url: string
        new(id, categoryId, name, address, phone, timework, url) = { id = id; categoryId = categoryId; name = name; address=address; phone = phone; timework = timework; url = url  }
        new(id, categoryId, name, address, phone, timework) = { id = id; categoryId = categoryId; name = name; address=address; phone = phone; timework = timework; url = null  }
        new(id, categoryId, name, address, phone) = { id = id; categoryId = categoryId; name = name; address=address; phone = phone; timework = null; url = null  }
        new(id, categoryId, name, address) = { id = id; categoryId = categoryId; name = name; address=address; phone = null; timework = null; url = null  }
        member public x.Id
                   with get() = x.id
                   and set(id: int) = x.id <- id
        member public x.CategoryId
                   with get() = x.categoryId
                   and set(categoryId: int) = x.categoryId <- categoryId
        member public x.Name
                   with get() = x.name
                   and set(name: string) = x.name <- name
        member public x.Address
                   with get() = x.address
                   and set(address: string) = x.address <- address
        member public x.Phone
                   with get() = x.phone
                   and set(phone: string) = x.phone <- phone
        member public x.Timework
                   with get() = x.timework
                   and set(timework: string) = x.timework <- timework
        member public x.Url
                   with get() = x.url
                   and set(url: string) = x.url <- url
     end

type CompanyEmail = 
    {
        CompanyId: int
        Email: string
    }

type CompanyDbItem = 
    {
        CompanyId: int
        SiteUrl: string
        BadUrl:bool
        EmailProcessed:bool
    }

type CountryItem = 
    {
        CountryId: int
        Name: string
        FName: string
        Capital: string
        Phone: string
        Domain: string
        Region: string
    }

type RegionItem = 
    {
        Code: string
        Name: string
    }

type CityItem = 
    {
        Name: string
        Subordination: string
        Code: string
        Region: string
    }

let regExpSpanTimeOut = TimeSpan.FromSeconds(15.0)

let monitor = new Object()

let lock (lockobj:obj) f =
    Monitor.Enter lockobj
    try
      f()
    finally
      Monitor.Exit lockobj

let getDivRow (html:HtmlDocument, resultList:List<TreeNode>) = 
    let rootNodeId = ref 0
    let nodeId = ref 1000
    let nodeHrefId = ref 100000
    html.Descendants ["div"]
    |> Seq.iter (fun x -> 
                         if x.HasClass("header") then 
                            match x.TryGetAttribute("class") with
                            | None -> printfn("ignore header")
                            | Some att ->
                                match att.Value() with
                                | "header" -> rootNodeId := !rootNodeId + 1
                                              resultList.Add(TreeNode(!rootNodeId, x.InnerText(), "77"))
                                | _ -> printfn("ignore header")
                         if x.HasClass("item") && x.InnerText().EndsWith(": ") then 
                            nodeId := !nodeId + 1
                            resultList.Add(TreeNode(!nodeId, x.InnerText(), !rootNodeId, null, "77"))
                         if x.HasClass("item") then
                             x |> (fun m ->
                                       let links = 
                                           m.Descendants ["a"]
                                           |> Seq.choose (fun x -> 
                                                  x.TryGetAttribute("href")
                                                  |> Option.map (fun a -> x.InnerText(), a.Value())
                                           )
                                       links
                                       |> Seq.map (fun (name, url) -> name, url)
                                       |> Seq.iter (fun (name, url) -> nodeHrefId := !nodeHrefId + 1
                                                                       resultList.Add(TreeNode(!nodeHrefId, name.Trim(), !nodeId, url.Trim(), "77"))  )   
                                   )
                         ) 

let getCategoryLinks (html:HtmlDocument, baseUrl: string, cityCode:string) =
    let rootNodeId = ref 0
    let nodeWithLink = ref 100000
    let resultList = List<TreeNode>()
    html.Descendants ["a"]
    |> Seq.iter (fun x -> if x.HasClass("header") then
                               match x.TryGetAttribute("class") with
                               | None -> printfn("ignore header")
                               | Some att ->
                                   match att.Value() with
                                   | "header" -> rootNodeId := !rootNodeId + 1
                                                 let curl = match x.TryGetAttribute("href") with
                                                            | None -> printfn("no href attribute")
                                                                      String.Empty
                                                            | Some att -> att.Value()
                                                 resultList.Add(TreeNode(!rootNodeId, x.InnerText(), 0, curl.Trim(), cityCode))

                                                 let doc = HtmlDocument.Load((sprintf "%s%s" baseUrl (curl.Trim())))
                                                 doc.Descendants ["div"]
                                                 |> Seq.iter (fun n -> if n.HasClass("subcats") then
                                                                          n |> (fun m ->
                                                                                        let links = 
                                                                                            m.Descendants ["a"]
                                                                                            |> Seq.choose (fun x -> x.TryGetAttribute("href")
                                                                                                                    |> Option.map (fun a -> x.InnerText(), a.Value())
                                                                                                          )
                                                                                        links
                                                                                        |> Seq.map (fun (name, url) -> name, url)
                                                                                        |> Seq.iter (fun (name, url) -> nodeWithLink := !nodeWithLink + 1
                                                                                                                        resultList.Add(TreeNode(!nodeWithLink, name.Trim(), !rootNodeId, url.Trim(), cityCode))  )   
                                                                                )
                                                             )
                                   | _ -> printfn("ignore header")  
                   ) 
    resultList

let getDivCompany (html:HtmlDocument, resultList:List<Company>, categoryId:int) = 
    let nodeId = ref 0
    html.Descendants ["div"]
    |> Seq.iter (fun x -> 
                         if x.HasClass("company") then 
                            match x.TryGetAttribute("class") with
                            | None -> printfn("ignore row")
                            | Some att ->
                                match att.Value() with
                                | "company" ->   
                                            x |> (fun m ->
                                                let links = 
                                                    m.Descendants ["a"]
                                                    |> Seq.choose (fun x -> 
                                                           x.TryGetAttribute("href")
                                                           |> Option.map (fun a -> x.InnerText(), a.Value())
                                                    )
                                                
                                                let company =
                                                    links
                                                    |> Seq.map (fun (name, url) -> name, url) |>  Seq.head |> (fun (x, _) -> x)

                                                let mutable address:string = null
                                                let mutable phone:string = null
                                                let mutable timework:string = null
                                                let mutable url:string = null
                                                x |> (fun m ->
                                                          m.Descendants ["li"]
                                                          |> Seq.iter (fun i ->  if (i.Elements("i") |>  Seq.head).HasClass("glyphicon glyphicon-map-marker") then                                              
                                                                                    address <- i.InnerText()
                                                                                 if (i.Elements("i") |>  Seq.head).HasClass("glyphicon glyphicon-phone-alt") then
                                                                                    phone <- i.InnerText()
                                                                                 if (i.Elements("i") |>  Seq.head).HasClass("glyphicon glyphicon-time") then
                                                                                     timework <- i.InnerText()
                                                                                 if (i.Elements("i") |>  Seq.head).HasClass("glyphicon glyphicon-share") then
                                                                                     url <- i.InnerText()
                                                                                 
                                                                       )
                                                      )
                                                nodeId := !nodeId + 1
                                                let company = if not(String.IsNullOrEmpty(company)) then company.Trim() else company
                                                let address = if not(String.IsNullOrEmpty(address)) then address.Trim() else address
                                                let phone = if not(String.IsNullOrEmpty(phone)) then phone.Trim() else phone
                                                let timework = if not(String.IsNullOrEmpty(timework)) then timework.Trim() else timework
                                                let url = if not(String.IsNullOrEmpty(url)) then url.Trim() else url
                                                resultList.Add(Company(!nodeId, categoryId, company, address, phone, timework, url))
                                            )

                                | _ -> printfn("ignore row")
    )

/// Decodes a Base64 string to a UTF8 string
let decodeBase64 text = 
  if (text:string).Length % 4 <> 0 then "" else
    // RFC 4648: The Base 64 Alphabet
    let A = [for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" -> c]
            |> List.mapi (fun i a -> a, i)
            |> Map.ofList

    // RFC 4648: The "URL and Filename safe" Base 64 Alphabet
    // let A = [for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=" -> c]
    //         |> List.mapi (fun i a -> a, i)
    //         |> Map.ofList

    let (.@) (m: Map<char, int>) key = try m.[key] with _ -> 0
  
    let quadToList ending (a, b, c, d) =
      let quad = (A.@ a &&& 0x3F <<< 18)
             ||| (A.@ b &&& 0x3F <<< 12)
             ||| (A.@ c &&& 0x3F <<<  6)
             ||| (A.@ d &&& 0x3F)
      let x = (quad &&& 0xFF0000) >>> 16
      let y = (quad &&& 0x00FF00) >>>  8
      let z = (quad &&& 0x0000FF)
      match ending with
      | 2 -> [byte x;]
      | 3 -> [byte x; byte y;]
      | _ -> [byte x; byte y; byte z;]
  
    let rec parse result input =
      match input with
      | a :: b ::'='::'=':: []   -> result @ quadToList 2 (a, b, '=', '=')
      | a :: b :: c ::'=':: []   -> result @ quadToList 3 (a, b,  c , '=')
      | a :: b :: c :: d :: tail -> parse (result @ quadToList 4 (a, b, c, d)) tail
      | _                        -> result

    [for c in text -> c]
    |> parse []
    |> List.toArray
    |> System.Text.Encoding.UTF8.GetString

let getDivCompanies (html:HtmlDocument, resultList:List<Company>, categoryId:int) = 
    let nodeId = ref 0
    let links = html.Descendants ["a"]
    html.Descendants ["div"]
    |> Seq.iter (fun x -> if x.HasClass("list-item hover") then 
                             nodeId := !nodeId + 1
                             let link = x.Descendants ["a"] |> Seq.last
                             let mutable company = Company(!nodeId, categoryId, link.InnerText(), null, null, null)
                             
                             x.Descendants ["div"]
                             |> Seq.iter(fun n -> if n.HasClass("row") then
                                                     let mutable key:string = null
                                                     let mutable value:string = null
                                                     n.Descendants ["div"] 
                                                     |> Seq.iter(fun n -> if n.HasClass("left") then
                                                                             key <- n.InnerText()
                                                                          if n.HasClass("right") then
                                                                             let span = n.Descendants ["span"]
                                                                             if not(Seq.isEmpty span) then 
                                                                                 value <- decodeBase64 ((span |> Seq.last).AttributeValue("data-link"))
                                                                             else
                                                                                 value <- n.InnerText() )
                                                     if key <> null && value <> null then
                                                        match key with
                                                        | "Адрес:" -> company.Address <- value
                                                        | "Телефон:" -> company.Phone <- value
                                                        | "Часы работы:" -> company.Timework <- value
                                                        | "Сайт:" -> company.Url <- value
                                                        | _ -> ()
                                                     )
                             resultList.Add(company)
                             )
  
let getPageNumber (html:HtmlDocument, tag:string) = 
    let mutable pagen:int=0
    html.Descendants [tag]
    |> Seq.filter (fun x -> x.HasClass("pagination") )
    |> Seq.iter (fun x -> 
                         x |> (fun m ->
                                 let links = 
                                     m.Descendants ["a"]
                                     |> Seq.choose (fun x -> 
                                        x.TryGetAttribute("href")
                                        |> Option.map (fun a -> x.InnerText(), a.Value())
                                     )
                                 let page =
                                     links
                                     |> Seq.map (fun (name, url) -> name, url) |>  Seq.last |> (fun (x, _) -> x)
                                 pagen <- page |> int
                                 ))
    pagen

let getCountryList (html:HtmlDocument) = 
    let resultList = new List<CountryItem>()
    html.Descendants ["table"]
    |> Seq.iter (fun x -> 
                          if x.HasClass("country-list") then 
                            x |> (fun m ->
                                      m.Descendants ["tr"]
                                      |> Seq.skip 1
                                      |> Seq.iter (fun tr -> let td = ((tr.Elements "td") |> Seq.toArray)
                                                             resultList.Add({ CountryId = 0; Name = td.[1].InnerText(); FName = td.[2].InnerText(); 
                                                                              Capital = td.[3].InnerText(); Phone = td.[4].InnerText();
                                                                              Domain = td.[5].InnerText(); Region = td.[6].InnerText() })
                                                            )
                                      ()
                                    )
                          )
    resultList

let getRussionRegionList (html:HtmlDocument) = 
    let resultList = new List<RegionItem>()
    html.Descendants ["table"]
    |> Seq.iter (fun x -> if x.HasAttribute("dir","ltr") && x.HasAttribute("style","table-layout:fixed;font-size:13px;font-family:arial,sans,sans-serif;border-collapse:collapse;border:none") then 
                             x |> (fun m ->
                                      m.Descendants ["tr"]
                                      |> Seq.iter (fun tr -> let td = ((tr.Elements "td") |> Seq.toArray)
                                                             resultList.Add({ Code = td.[0].InnerText(); Name = td.[1].InnerText(); })
                                                            )
                                    )
                          )
    resultList

let getRussionCitiesList (html:HtmlDocument) = 
    let resultList = new List<CityItem>()
    html.Descendants ["table"]
    |> Seq.iter (fun x -> if x.HasId("table2") && x.HasAttribute("dir","LTR") then 
                             x |> (fun m ->
                                      m.Descendants ["tr"]
                                      |> Seq.skip 1
                                      |> Seq.iter (fun tr -> let td = ((tr.Elements "td") |> Seq.toArray)
                                                             if Seq.length td =5 then
                                                                resultList.Add({  Name = td.[0].InnerText(); Subordination =td.[1].InnerText(); Code = td.[2].InnerText(); Region = td.[3].InnerText();})
                                                            )
                                    )
                          )
    resultList
    
let links (html:HtmlDocument, tagName: string) = 
    html.Descendants [tagName]
    |> Seq.choose (fun x -> x.TryGetAttribute("href")
                            |> Option.map (fun a -> x.InnerText(), a.Value())
                   )   
                   


[<Literal>]
let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=CountryDictionary;Integrated Security=True;Connect Timeout=3;Min Pool Size=100;Max Pool Size=1000;Pooling=true;"
//let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=tmp;Integrated Security=True;Connect Timeout=3;Min Pool Size=100;Max Pool Size=1000;Pooling=true;"

type sql = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER,
                           connectionString,
                           IndividualsAmount = 1000,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>


let insertCategoryData (name, parentId:int, url:string, cityCode: string) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)

    let qry = 
        query {
            for cust in ctx.Dbo.TblIndustryCategory do
            where (cust.Name = name && cust.CityCode = cityCode)
        }
    if (Seq.isEmpty qry) then
       let category = ctx.Dbo.TblIndustryCategory
       let row = category.Create()
       row.Name <- name
       if parentId > 0 then
          row.ParentIndustryCategoryId <- parentId
       if not (String.IsNullOrEmpty(url)) then
          row.Url <- url
       if not (String.IsNullOrEmpty(cityCode)) then
          row.CityCode <- cityCode
       ctx.SubmitUpdates()
    else
      printfn "Category already exists ( Name - %s, CityCode - %s )" name cityCode

let insertCountryData (list:List<CountryItem>) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    list |> List.ofSeq |> Seq.sortBy(fun x -> x.Name)
    |> Seq.iter(fun x->
                       let qry = 
                           query {
                               for country in ctx.Dbo.TblCountry do
                               select country.Name
                               contains x.Name
                           }
                       if not qry then
                          let country = ctx.Dbo.TblCountry
                          let row = country.Create()
                          row.Name <- x.Name
                          row.FName <- x.FName
                          row.Capital <- x.Capital
                          row.Phone <- x.Phone
                          row.Domain <- x.Domain
                          row.Region <- x.Region
                       else
                         printfn "Country already exists %s" x.Name       
    )
    ctx.SubmitUpdates()
    

let insertCompanyData (name, address:string, phone:string, timework:string, url:string, categoryId:int, ignoreCheck:bool) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)

    let qry = 
        query {
            for cust in ctx.Dbo.TblCompany do
            select cust.Name
            contains name
        }
    if not qry || ignoreCheck then
       let category = ctx.Dbo.TblCompany
       let row = category.Create()
       row.Name <- name
       if not (String.IsNullOrEmpty(address)) then
          row.Address <- address
       if not (String.IsNullOrEmpty(phone)) then
          row.Phone <- phone
       if not (String.IsNullOrEmpty(timework)) then
          row.Timework <- timework
       if not (String.IsNullOrEmpty(url)) then
          row.SiteUrl <- url
       if categoryId > -1 then
          row.IndustryCategoryId <- categoryId
       ctx.SubmitUpdates()
    else
      printfn "Company already exists: %s" name

let insertBulkCompanyData (list :List<Company>) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    list |> Seq.iter(fun c ->
        let category = ctx.Dbo.TblCompany
        let row = category.Create()
        row.Name <- c.Name
        if not (String.IsNullOrEmpty(c.Address)) then
           row.Address <- c.Address
        if not (String.IsNullOrEmpty(c.Phone)) then
           row.Phone <- c.Phone
        if not (String.IsNullOrEmpty(c.Timework)) then
           row.Timework <- c.Timework
        if not (String.IsNullOrEmpty(c.Url)) then
           row.SiteUrl <- c.Url
        if c.CategoryId > -1 then
           row.IndustryCategoryId <- c.CategoryId
    )
    ctx.SubmitUpdates()

let insertBulkRegionData (list :seq<RegionItem>) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    list |> Seq.iter(fun c ->
        let qry = 
            query {
                for reg in ctx.Dbo.TblRegion do
                select reg.Name
                contains c.Name
            }
        if not qry then
            let region = ctx.Dbo.TblRegion
            let row = region.Create()
            row.Name <- c.Name
            row.Code <- c.Code
        else
            printfn "Region already exists: %s %s"  c.Code c.Name 
    )
    ctx.SubmitUpdates()

let insertBulkCityData (list :seq<CityItem>) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    list |> Seq.iter(fun c ->
        let qry = 
            query {
                for city in ctx.Dbo.TblCity do
                select city.Name
                contains c.Name
            }
        if not qry then
            let city = ctx.Dbo.TblCity
            let row = city.Create()
            row.Name <- c.Name
            row.Subordination <- c.Subordination
            row.Code <- c.Code
            row.Region <- c.Region
        else
            printfn "City already exists: %s" c.Name 
    )
    ctx.SubmitUpdates()

let updateCompanyEmailProcessed (companyId:int, processed:bool) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    try
        let ctx = sql.GetDataContext(TransactionOptions)
        
        query {
                for company in ctx.Dbo.TblCompany do
                where (company.CompanyId = companyId)
        }
        |> Seq.iter( fun e -> e.EmailProcessed <- processed )

        ctx.SubmitUpdates()
    with
        | _ as ex -> printfn "updateCompanyBadUrl -%d error - %s" companyId ex.Message
    

let insertCompanyEmail (list:seq<CompanyEmail>) = 
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)

    list 
    |> Seq.iter (fun x ->
            let qry = 
                query {
                    for eml in ctx.Dbo.TblEmail do
                    select eml.Email
                    contains x.Email
                }
            if not qry then
                let email = ctx.Dbo.TblEmail
                let erow = email.Create()
                erow.Email <- x.Email
                ctx.SubmitUpdates()
                let compeml = ctx.Dbo.TblCompanyEmail
                let row = compeml.Create()
                row.CompanyId <- x.CompanyId
                row.EmailId <- erow.EmailId
                ctx.SubmitUpdates()
            else
                let email = query {
                    for eml in ctx.Dbo.TblEmail do
                    where (eml.Email = x.Email)
                    select eml
                    exactlyOne
                }
                printfn "Email already exists: origin - %s db - %s %d" x.Email email.Email email.EmailId
                let qry = 
                    query {
                        for ceml in ctx.Dbo.TblCompanyEmail do
                        where (ceml.CompanyId = x.CompanyId && ceml.EmailId = email.EmailId)
                        select ceml
                    }
                if Seq.length qry = 0 then
                    let compeml = ctx.Dbo.TblCompanyEmail
                    let row = compeml.Create()
                    row.CompanyId <- x.CompanyId
                    row.EmailId <- email.EmailId
                    ctx.SubmitUpdates()
    )

let updateReferenceData (name , parentId:int) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for cust in ctx.Dbo.TblIndustryCategory do
        where (cust.Name = name)
    }
    |> Seq.iter( fun e ->
        e.ParentIndustryCategoryId <- parentId
    )
    ctx.SubmitUpdates()

let updateCategoryCompanyProcessed (categoryId:int, processed:bool) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for cust in ctx.Dbo.TblIndustryCategory do
        where (cust.IndustryCategoryId = categoryId)
    }
    |> Seq.iter( fun e ->
        e.CompanyProcessed <- processed
    )
    ctx.SubmitUpdates()

let getCategoryId(name, cityCode) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for category in ctx.Dbo.TblIndustryCategory do
        where (category.Name = name && category.CityCode = cityCode)
        select category
        exactlyOne
        }

let getCompanies =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for company in ctx.Dbo.TblCompany do
        select company
        } |> Seq.map (fun x -> { CompanyId = x.CompanyId; SiteUrl = x.SiteUrl; BadUrl = x.BadUrl; EmailProcessed = x.EmailProcessed })

let updateCompanyBadUrl (companyId:int, badUrl:bool, processed:bool, finded: bool) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    try
        let ctx = sql.GetDataContext(TransactionOptions)
        query {
            for company in ctx.Dbo.TblCompany do
            where (company.CompanyId = companyId)
        }
        |> Seq.iter( fun e ->
            e.BadUrl <- badUrl
            e.EmailProcessed <- processed
            e.EmailFinded <- finded
        )
        ctx.SubmitUpdates()
    with
        | _ as ex -> printfn "updateCompanyBadUrl -%d error - %s" companyId ex.Message
    

let trunc(str:string) = 
    str.Substring(0, (System.Math.Min(str.Length, 250)))

let getBaseUrl(url:string) = 
    let uri = new Uri(url)
    let baseUrl = uri.GetLeftPart(System.UriPartial.Authority)
    baseUrl

let getHost(baseUrl:string)=
    let mutable host:string=null
    let uri = Uri(baseUrl);   
    host <- uri.Host.TrimStart("www.".ToCharArray())  
    let index = host.LastIndexOf(".")
    if (index > 0) then
       host <- host.Substring(0, index)
    host

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
            if (statusCode >= 300 && statusCode <= 399) && not(String.IsNullOrEmpty(location)) && location <> url && (url.Length <= 80 && location.Length <= 80) && not(location.StartsWith("://")) then
                redirectUri <- response.Headers.Location
                redirectUri <- if not(redirectUri.IsAbsoluteUri) then new Uri(new Uri((new Uri(url)).GetLeftPart(UriPartial.Authority)), redirectUri) else redirectUri 
                printfn "getHtmlAsStringAsync(url:string) Redirecting to %s" (redirectUri.ToString())
                if linkRedirection.Add(redirectUri.ToString()) then
                    let! content = getHtmlAsync(redirectUri.ToString())
                    return content
                else
                    linkRedirection.Clear()
                    return raise (System.ArgumentException("Cycled redirect location detected!"))
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

type MetaRegex = Regex< "<[META|meta](?!\s*(?:name|value)\s*=)[^>]*?charset\s*=[\s\"']*([a-zA-Z0-9-]+)[\s\"'\/]*>", noMethodPrefix = true >

let getEncoding(html:string) =
    let mutable result:string = "utf-8"
    let meta = MetaRegex().Matches(html)

    if not(Seq.isEmpty meta) then
        let data = meta |> Seq.map (fun x -> x.Value)
                        |> Seq.head 
        try
            let index = data.IndexOf("charset=")
            if index > 0 then
               let charSet = data.Substring(index + 8).Trim("\'\"/> ".ToCharArray())
               result <- charSet 
        with
            | _ -> result <- result
    result    
    
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
                        updateCompanyEmailProcessed(companyId, true)
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
                        else
                            updateCompanyBadUrl (companyId, false, true, false);

                        //if (Seq.isEmpty dataLink) && (Seq.isEmpty data) && (Seq.isEmpty mailto) && (Seq.isEmpty emails) then
                        //    randomCrawl url companyId visitedEmailExtractor |> Async.RunSynchronously *)
                else
                    printfn "asyncEmailCrawler bad url - %s " url 
                    updateCompanyBadUrl(companyId, true, true, false)
        with
        | :? System.Net.WebException as ex ->
                printfn "asyncEmailCrawler w url - %s error - %s" url  (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
        | :? HttpRequestException as ex -> 
                printfn "asyncEmailCrawler h url - %s error - %s" url (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
        | :? System.Net.CookieException as ex ->
                printfn "asyncEmailCrawler c url - %s error - %s" url (trunc(ex.Message))
                updateCompanyBadUrl (companyId, true, true, false)
        | _ as ex -> printfn "asyncEmailCrawler g url - %s error - %s" url (trunc(ex.Message))
                     updateCompanyBadUrl (companyId, true, true, false)
    }

let emailCrawler (url:string, companyId: int) = 
    asyncEmailCrawler(url, companyId) |> Async.RunSynchronously

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

let isValidEmail(email:string)=
    try
        let _ = new System.Net.Mail.MailAddress(email);
        true && (Regex.IsMatch(email, @"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"))
    with
        | _ -> false

/// Colored printf

let cprintf c fmt = 
    Printf.kprintf
        (fun s ->
            let old = System.Console.ForegroundColor
            try
              System.Console.ForegroundColor <- c;
              System.Console.Write s
            finally
              System.Console.ForegroundColor <- old)
        fmt
// Colored printfn
let cprintfn c fmt =
    cprintf c fmt
    printfn ""

let trimEmailWithSubject(email:string)=
    let index = email.LastIndexOf("?subject")
    if (index > 0) then
       email.Substring(0, index)
    else
       email

[<EntryPoint>]
let main argv =
    System.AppDomain.CurrentDomain.UnhandledException.AddHandler(
        fun _ (args:System.UnhandledExceptionEventArgs) ->
           lock monitor (fun () ->
                 printfn "UnhandledException %s" <| (args.ExceptionObject :?> System.Exception).ToString()
              )
    )

    Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    //let res = decodeBase64 "aHR0cDovL3d3dy4xMTJzdG8ucnU="

    //let dt = System.Convert.ToBase64String [|"https://sto-parnas.ru/".getBy|]
    //emailCrawler("http://www.bit-medic.ru/", 127)
    //emailCrawler("https://www.mebell.ru", 127)
    //let companyList = new List<Company>()
    //let doc = HtmlDocument.Load("https://spb.spravker.ru/shinomontazh/")
    //getDivCompanies(doc, companyList, 12)

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
                           )

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
                                    && notSocialNetwork(x.SiteUrl)
                                     )
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
    printfn "Finished. Company count - %d." (Seq.length resultCompany)
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
