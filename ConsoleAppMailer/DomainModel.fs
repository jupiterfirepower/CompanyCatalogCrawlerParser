module DomainModel

[<Struct>]
type Email = 
     val mutable Email: string
     val mutable EmailId : int
     val mutable BadIgnoreEmail : bool
     new(email: string, emailid:int, badignoreemail:bool) = { Email = email; EmailId = emailid; BadIgnoreEmail = badignoreemail }

