module Utils

open System
open FSharp.Data.Sql
open FSharp.Text.RegexProvider
open System.Threading
open System.Threading.Tasks

let regExpSpanTimeOut = TimeSpan.FromSeconds(15.0)

let monitor = new Object()

let lock (lockobj:obj) f =
    Monitor.Enter lockobj
    try
      f()
    finally
      Monitor.Exit lockobj

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

let translitCyrillicToLatin(words:string)=
    let decoder = [ ("а", "a");("б", "b");("в", "v");("г", "g");("д", "d");("е", "e");("ё", "yo");("ж", "zh");
                    ("з", "z"); ("и", "i"); ("й", "j"); ("к", "k"); ("л", "l"); ("м", "m"); ("н", "n");
                    ("о", "o"); ("п", "p"); ("р", "r"); ("с", "s"); ("т", "t"); ("у", "u"); ("ф", "f");
                    ("х", "h"); ("ц", "ts"); ("ч", "ch"); ("ш", "sh"); ("щ", "sch"); ("ъ", ""); ("ы", "i");
                    ("ь", ""); ("э", "e"); ("ю", "yu"); ("я", "ya"); ("А", "A"); ("Б", "B"); ("В", "V");
                    ("Г", "G"); ("Д", "D"); ("Е", "E"); ("Ё", "Yo"); ("Ж", "Zh");("З", "Z"); ("И", "I");
                    ("Й", "J"); ("К", "K"); ("Л", "L"); ("М", "M"); ("Н", "N"); ("О", "O"); ("П", "P");
                    ("Р", "R"); ("С", "S"); ("Т", "T"); ("У", "U"); ("Ф", "F"); ("Х", "H"); ("Ц", "TS");
                    ("Ч", "Ch"); ("Ш", "Sh"); ("Щ", "Sch");("Ъ", ""); ("Ы", "I"); ("Ь", ""); ("Э", "E");
                    ("Ю", "Yu"); ("Я", "Ya"); ("-", "-"); (" ", "-");]
    let mutable result:string=""
    for char in words do
        let (_, latin) = decoder |> List.find (fun (cyr, _) -> cyr = (sprintf "%c" char))
        result <- result + latin
    result

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




