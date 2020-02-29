namespace DataStructure

open System
open System.Threading
open System.IO
open System.Drawing
open System.Drawing.Drawing2D
open System.Drawing.Imaging
open FSharp.Data

module Helpers =
    type MutableList<'item when 'item:equality>(init) =
        let mutable items: 'item list = init
    
        member _.Value = items
    
        member x.Update updater =
            let current = items
            let newItems = updater current
            if not <| obj.ReferenceEquals(current, Interlocked.CompareExchange(&items, newItems, current))
                then x.Update updater
                else x
    
        member x.Add item = x.Update (fun L -> item::L)
        member x.Remove item = x.Update (fun L -> List.filter (fun i -> i <> item) L)
        member _.Contains item = let current = items |> List.map(fun x -> x)
                                 List.contains item current
    
        static member empty = new MutableList<'item>([])
        static member add item (l:MutableList<'item>) = l.Add item
        static member get (l:MutableList<'item>) = l.Value
        static member remove item (l:MutableList<'item>) = l.Remove item

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
                          System.Threading.Thread.Sleep(sleep)
                          loop(n-1)
              loop max

    let rec allFiles dirs =
        if Seq.isEmpty dirs then Seq.empty else
             seq { yield! dirs |> Seq.collect Directory.EnumerateFiles
                   yield! dirs |> Seq.collect Directory.EnumerateDirectories |> allFiles }

    let getAllFilesFromDir path =
        Directory.EnumerateFiles(path, "*.msg", SearchOption.AllDirectories)

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


    let Resize(fromfilepath:string, width:int, height:int, outfilepath:string):Bitmap = 
        let destRect = new Rectangle(0, 0, width, height)
        let destImage = new Bitmap(width, height)
        try
           let image = Image.FromFile(fromfilepath)
           let ext = Path.GetExtension(outfilepath).ToLower()
           if ext <> ".gif" then
              destImage.SetResolution(image.HorizontalResolution, image.VerticalResolution)

              use graphics = Graphics.FromImage(destImage) 
              graphics.CompositingMode <- CompositingMode.SourceCopy
              graphics.CompositingQuality <- CompositingQuality.HighQuality
              graphics.InterpolationMode <-  InterpolationMode.HighQualityBicubic
              graphics.SmoothingMode <- SmoothingMode.HighQuality
              graphics.PixelOffsetMode <- PixelOffsetMode.HighQuality

              let wrapMode = new ImageAttributes()
              wrapMode.SetWrapMode(WrapMode.TileFlipXY)
              graphics.DrawImage(image, destRect, 0, 0, image.Width, image.Height, GraphicsUnit.Pixel, wrapMode)
              image.Dispose()
        
              let format = function
                           | ".png" -> Some ImageFormat.Png
                           | ".bmp" -> Some ImageFormat.Bmp
                           | ".emf" -> Some ImageFormat.Emf
                           | ".exif" -> Some ImageFormat.Exif
                           | ".icon" -> Some ImageFormat.Icon
                           | ".jpeg" -> Some ImageFormat.Jpeg
                           | ".jpg" -> Some ImageFormat.Jpeg
                           | ".jpe" -> Some ImageFormat.Jpeg
                           | ".jif" -> Some ImageFormat.Jpeg
                           | ".jfif" -> Some ImageFormat.Jpeg
                           | ".jfi" -> Some ImageFormat.Jpeg
                           | ".tiff" -> Some ImageFormat.Tiff
                           | ".tif" -> Some ImageFormat.Tiff
                           | ".wmf" -> Some ImageFormat.Wmf
                           | _ -> None

              match format ext with
                    | Some f -> destImage.Save(outfilepath, f)
                    | None -> ()
        with 
           | _ as ex -> printfn "Error : %s" ex.Message
        destImage


    let resizeImagesInDoc (html:HtmlDocument, rootDirForEmailImages) = 
        html.Descendants ["img"]
        |> Seq.iter (fun x -> let w = x.AttributeValue("width")
                              let h = x.AttributeValue("height")
                              let src = x.AttributeValue("src")
                              let regexp = System.Text.RegularExpressions.Regex("cid:(.*)@")
                              let v = regexp.Match(src)
                              let cname = v.Groups.[1].ToString()
                              let fileName = sprintf @"%s\%s" rootDirForEmailImages cname
                              Resize(fileName, w |> int, h |> int, fileName) |> ignore
                                  )
