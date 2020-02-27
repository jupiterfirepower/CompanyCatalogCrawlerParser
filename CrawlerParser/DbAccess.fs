module DbAccess

open System
open FSharp.Data
open FSharp.Data.Sql
open System.Collections.Generic
open DomainModel

[<Literal>]
let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=CountryDictionary;Integrated Security=True;Connect Timeout=3;"
//let connectionString = "Data Source=DESKTOP-M6ISP61\SQLEXPRESS; Initial Catalog=CountryDictionary;Integrated Security=True;Connect Timeout=3;Min Pool Size=100;Max Pool Size=1000;Pooling=true;"
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

(*
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
*)    

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

let checkCategoryCityCode(cityCode) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for category in ctx.Dbo.TblIndustryCategory do
        select category.CityCode
        contains cityCode
        }


let getCity(name) =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for city in ctx.Dbo.TblCity do
        where (city.Name = name)
        select city
        exactlyOne
        }

let getCompanies =
    let TransactionOptions = { FSharp.Data.Sql.Transactions.IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.DontCreateTransaction; FSharp.Data.Sql.Transactions.Timeout = TimeSpan.FromSeconds(1.0)}
    let ctx = sql.GetDataContext(TransactionOptions)
    query {
        for company in ctx.Dbo.TblCompany do
        select company
        } |> Seq.map (fun x -> { CompanyId = x.CompanyId; SiteUrl = x.SiteUrl; BadUrl = x.BadUrl; EmailProcessed = x.EmailProcessed; EmailFinded = x.EmailFinded })

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


let updateCompanyFlags(companyId:int, badUrl:bool, processed:bool, finded: bool) =
    do
        use cmd = new FSharp.Data.SqlCommandProvider<"
            UPDATE [CountryDictionary].[dbo].[tblCompany]
            SET [EmailProcessed]=@processed,
                [BadUrl]=@badUrl,
                [EmailFinded]=@finded
            
            WHERE [CompanyId] = @companyId 
            " , connectionString>(connectionString)
    
        cmd.Execute(processed = processed, badUrl = badUrl, finded = finded, companyId = companyId) |> ignore

