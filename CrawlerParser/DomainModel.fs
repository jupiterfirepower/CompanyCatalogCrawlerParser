module DomainModel

open System.Threading

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
        EmailFinded:bool
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

