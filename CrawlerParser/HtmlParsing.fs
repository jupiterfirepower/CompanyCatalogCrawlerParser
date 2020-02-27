module HtmlParsing

open System
open FSharp.Data
open FSharp.Data.Sql
open System.Collections.Generic
open DomainModel
open Utils

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

let getNovCategoryLinks (html:HtmlDocument, baseUrl: string, cityCode:string) = 
    let rootNodeId = ref 0
    let nodeHrefId = ref 100000
    let resultList = List<TreeNode>()
    html.Descendants ["div"]
    |> Seq.iter (fun x -> 
                         if x.HasClass("cats-list row") then 
                            x.Descendants ["div"]
                            |> Seq.iter (fun c -> if c.HasClass("col-xs-12 col-sm-6 col-md-6 col-lg-4") then 
                                                     c.Descendants ["h3"] 
                                                     |> Seq.iter (fun h -> h.Descendants ["a"] 
                                                                           |> Seq.iter(fun c -> rootNodeId := !rootNodeId + 1
                                                                                                resultList.Add(TreeNode(!rootNodeId, c.InnerText(), 0, null, cityCode))
                                                                                      )
                                                                 )
                                                     c.Descendants ["a"]
                                                     |> Seq.iter (fun l -> nodeHrefId := !nodeHrefId + 1
                                                                           resultList.Add(TreeNode(!nodeHrefId, l.InnerText(), !rootNodeId, l.AttributeValue("href"), cityCode))
                                                                           if l.HasClass("show_all") then 
                                                                               let chref = l.AttributeValue("href").Trim().Replace("//","/")

                                                                               let mutable url:string = null
                                                                               if baseUrl.EndsWith("/") then
                                                                                  url <- (sprintf "%s%s" (baseUrl.TrimEnd("/".ToCharArray())) (chref)) 
                                                                               else
                                                                                  url <- (sprintf "%s%s" baseUrl (chref)) 
                                                                               let doc = HtmlDocument.Load(url)
                                                                               doc.Descendants ["div"]
                                                                               |> Seq.iter (fun n -> if n.HasClass("cat-item") then
                                                                                                        n.Descendants ["a"]
                                                                                                        |> Seq.iter (fun a -> nodeHrefId := !nodeHrefId + 1
                                                                                                                              let trNode = TreeNode(!nodeHrefId , a.InnerText(), !rootNodeId, a.AttributeValue("href"), cityCode)
                                                                                                                              if not(resultList.Exists(fun item -> item.Name = a.InnerText() && 
                                                                                                                                                                   item.CityCode = cityCode && item.ParentId = !rootNodeId 
                                                                                                                                                                   && item.Href = a.AttributeValue("href") )) then 
                                                                                                                                 resultList.Add(trNode)
                                                                                                                     )
                                                                                           )
                                                                   )
                                        )
                            
                         ) 
    resultList

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

let getNovDivCompanies (html:HtmlDocument, resultList:List<Company>, categoryId:int) = 
    let nodeId = ref 0
    let links = html.Descendants ["a"]
    html.Descendants ["div"]
    |> Seq.iter (fun x -> if x.HasClass("org") then 
                             nodeId := !nodeId + 1
                             let companyName = x.Descendants ["h3"] |> Seq.last
                             let site = x.Descendants ["a"] |> Seq.last
                             let mutable company = Company(!nodeId, categoryId, companyName.InnerText(), null, null, null)
                             company.Url <- site.InnerText().Trim()
                             
                             let data = x.Descendants ["ul"]
                             data
                             |> Seq.iter(fun n -> if n.HasClass("address") then
                                                     let mutable key:string = null
                                                     let mutable value:string = null
                                                     n.Descendants ["p"] 
                                                     |> Seq.iter(fun m -> let atrv = m.AttributeValue("data-lnk")
                                                                          if atrv <> null && not(String.IsNullOrEmpty(atrv)) then
                                                                             company.Url <- decodeBase64(atrv.TrimStart("b'".ToCharArray()).TrimEnd("'".ToCharArray()))

                                                                          m.Descendants ["span"] 
                                                                          |> Seq.iter(fun sp -> if sp.HasClass("nm") then
                                                                                                   key <- sp.InnerText().Trim()
                                                                                                if sp.HasClass("value") then
                                                                                                   value <- sp.InnerText().Trim()
                                                                                                    )

                                                                          if key <> null && value <> null then
                                                                            match key with
                                                                            | "адрес:" -> company.Address <- value
                                                                            | "телефон:" -> company.Phone <- value
                                                                            | "график (часы) работы:" -> company.Timework <- value
                                                                            | "электронная почта:" -> ()
                                                                            | _ -> ()
                                                                 )
                                                     resultList.Add(company)
                                          )
                 )

let getCities (html:HtmlDocument) = 
    let mutable tableNum:int=0
    let resultList = List<CityItem>()
    html.Descendants ["table"]
    |> Seq.iter (fun x -> if x.HasAttribute("dir", "ltr") && x.HasAttribute("style", "table-layout:fixed;font-size:10pt;font-family:arial,sans,sans-serif;width:0px;border-collapse:collapse;border:none") then //style="table-layout:fixed;font-size:10pt;font-family:arial,sans,sans-serif;width:0px;border-collapse:collapse;border:none"
                             tableNum <- tableNum + 1
                             x.Descendants ["tr"]
                             |> Seq.iter(fun tr -> if tr.HasAttribute("style", "height:21px") then 
                                                      let nodes = tr.Descendants ["td"]
                                                      if Seq.length nodes > 2 then
                                                         let cnode = nodes |> Seq.skip(2) |> Seq.head
                                                         resultList.Add({ Name = cnode.InnerText().Replace("г.","").Trim(); Subordination = null; Code = string <| tableNum; Region = null })
                                                      ()
                                                      )
                       
                 )
    resultList
  
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

