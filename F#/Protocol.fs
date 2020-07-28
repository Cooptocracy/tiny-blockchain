module Protocol
open System
open System.Net
open System.Collections
open System.Collections.Generic
open System.IO
open System.Text
open System.Reactive.Linq
open System.Reactive.Subjects
open System.Reactive.Disposables
open System.Threading.Tasks
open System.Security.Cryptography
open FSharpx
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Validation
open FSharpx.Option
open NodaTime
open log4net
open Org.BouncyCastle.Crypto.Digests
open Org.BouncyCastle.Utilities.Encoders

type settings = AppSettings<"app.config">
let baseDir = settings.BaseDir
let version = 70001
let connectTimeout = TimeSpan.FromSeconds(float settings.ConnectTimeout)
let handshakeTimeout = TimeSpan.FromSeconds(float settings.HandshakeTimeout)
let commandTimeout = TimeSpan.FromSeconds(float settings.CommandTimeout)
let minGetdataBatchSize = settings.MinGetdataBatchSize
let maxGetdataBatchSize = settings.MaxGetdataBatchSize
let coinbaseMaturity = 100
let maxBlockSize = 1000000
let maxMoney = 2100000000000000L

let (|?|) = defaultArg // infix version
let txInvType = 1
let blockInvType = 2
let filteredBlockInvType = 3
let zeroHash: byte[] = Array.zeroCreate 32
let random = new Random() // used to produce nonces in ping/pong
exception ValidationException of string
let either = new EitherBuilder()
let maybe = new MaybeBuilder()

let rec iterate f value = seq { // Same as iterate in clojure
    yield value
    yield! iterate f (f value) }

(** Find the first byte that matches a given condition starting from the end of the array *)
let revFind (arr: byte[]) (f: byte -> bool) =
    let rec rf (i: int) =
        if i < 0 || f(arr.[i]) then 
            i
        else
            rf (i-1)
    rf (arr.Length-1)

let logger = LogManager.GetLogger("bitcoinfs")
type ILog with
    member x.DebugF format = Printf.ksprintf (x.Debug) format
    member x.InfoF format = Printf.ksprintf (x.Info) format
    member x.ErrorF format = Printf.ksprintf (x.Error) format
    member x.FatalF format = Printf.ksprintf (x.Fatal) format

type HashCompare() = 
    interface IEqualityComparer<byte[]> with
        member x.Equals(left: byte[], right: byte[]) = StructuralComparisons.StructuralEqualityComparer.Equals(left, right)
        member x.GetHashCode(hash: byte[]) = StructuralComparisons.StructuralEqualityComparer.GetHashCode hash

type Async =
    static member AsObservable(op: Async<'a>) =
        Observable.Create(fun (obs: IObserver<'a>) ->
            Async.StartWithContinuations(op, (fun r -> obs.OnNext r; obs.OnCompleted()), obs.OnError, obs.OnError)
            Disposable.Create(fun () -> ignore())
        )

type Option =
    static member iterSecond(f: unit -> unit) (opt: Option<'a>) = 
        if opt.IsNone then f()
        opt
let errorIfFalse (errorMsg: string) (b: bool) = b |> Option.ofBool |> Option.iterSecond(fun() -> logger.Debug errorMsg)

let hashBlock (digest: unit -> HashAlgorithm) (input: byte[]): byte[] = digest().ComputeHash(input)

let ripe160 = hashBlock (fun () -> new RIPEMD160Managed() :> HashAlgorithm)
let sha1 = hashBlock (fun () -> new SHA1Managed() :> HashAlgorithm)
let sha256 = hashBlock (fun () -> new SHA256Managed() :> HashAlgorithm)
let hash160 = sha256 >> ripe160
let dsha = sha256 >> sha256

let hashFromHex(s: String) = 
    let h = Hex.Decode(s)
    Array.Reverse h
    h
let hashToHex(h: byte[]) = 
    let rh = h |> Array.copy |> Array.rev
    Hex.ToHexString rh

let ToBinaryArray(f: BinaryWriter -> unit) = 
    use ms = new MemoryStream()
    use os = new BinaryWriter(ms)
    f(os)
    ms.ToArray()

let ParseByteArray(b: byte[])(f: BinaryReader -> 'a) = 
    use ms = new MemoryStream(b)
    use reader = new BinaryReader(ms, Encoding.ASCII)
    f(reader)

type BinaryReader with 
    member x.ReadVarInt(): int = 
        let b = x.ReadByte()
        match b with 
        | 0xFDuy -> int(x.ReadUInt16())
        | 0xFEuy -> int(x.ReadUInt32())
        | 0xFFuy -> int(x.ReadUInt64())
        | _ -> int(b)
    member x.ReadVarString(): string = 
        Encoding.ASCII.GetString(x.ReadScript())
    member x.ReadScript(): byte[] =
        let length = x.ReadVarInt()
        x.ReadBytes(length)

type BinaryWriter with
    member x.WriteVarInt(i: int) = 
        if i <= 0xFC then
            x.Write(byte(i))
        elif i <= 0xFFFF then
            x.Write(byte(0xFD))
            x.Write(int16 i)
        else // Protocol allows for int64 but there isn't anything varint that goes so high
            x.Write(byte(0xFE))
            x.Write(int32 i)
    member x.WriteVarString(s: string) =
        x.WriteVarInt(s.Length)
        x.Write(Encoding.ASCII.GetBytes(s))
    member x.WriteScript(b: byte[]) =
        x.WriteVarInt(b.Length)
        x.Write(b)

let torPrefix = [|0xFDuy; 0x87uy; 0xD8uy; 0x7Euy; 0xEBuy; 0x43uy|]
let encodeTorAsIpV6 (onionAddress: string) = 
    let address = onionAddress.Replace(".onion", "")
    let addressBytes = Base32.Base32Encoder.Decode address
    let padding = 10-addressBytes.Length
    let ipv6AddressBytes = Array.concat [torPrefix; Array.zeroCreate padding; addressBytes]
    new IPAddress(ipv6AddressBytes)

let decodeAddressString address =
    let (success, myIP) = IPAddress.TryParse address
    if success then myIP else encodeTorAsIpV6 address

type NetworkAddr(ip: IPEndPoint) = 
    // the address put in the version message. The other side may want to connect back using this IP
    static member MyAddress = NetworkAddr(new IPEndPoint(decodeAddressString settings.MyExtIp, settings.ServerPort)) // TODO: Lookup external address
    member x.ToByteArray() =
        use ms = new MemoryStream()
        use os = new BinaryWriter(ms)
        os.Write(0L)
        os.Write(0L)
        os.Write(0xFFFF0000)
        os.Write(ip.Address.GetAddressBytes())
        os.Write(int16(ip.Port))
        ms.ToArray()

    static member Parse(reader: BinaryReader) = 
        reader.ReadBytes(8) |> ignore // skip services field
        let ip = reader.ReadBytes(16)
        let ipAddress = new IPAddress(ip)
        let port = int(uint16(IPAddress.NetworkToHostOrder(reader.ReadInt16())))
        new NetworkAddr(new IPEndPoint(ipAddress, port))

    member val EndPoint = ip with get

type Version(version: int32, services: int64, timestamp: int64, recv: byte[], from: byte[], nonce: int64, userAgent: string, height: int32, relay: byte) = 
    member x.ToByteArray() = ToBinaryArray (fun os ->
        os.Write(version)
        os.Write(services)
        os.Write(timestamp)
        os.Write(recv)
        os.Write(from)
        os.Write(nonce)
        os.WriteVarString(userAgent)
        os.Write(height)
        os.Write(relay)
        )

    static member Create(timestamp: Instant, recv: IPEndPoint, from: IPEndPoint, nonce: int64, userAgent: string, height: int32, relay: byte) = 
        new Version(version, 1L, timestamp.Ticks / NodaConstants.TicksPerSecond, (new NetworkAddr(recv)).ToByteArray(),
            (new NetworkAddr(from)).ToByteArray(), nonce, userAgent, height, relay)

    static member Parse(reader: BinaryReader) =
        let version = reader.ReadInt32()
        let services = reader.ReadInt64()
        let timestamp = reader.ReadInt64()
        let recv = reader.ReadBytes(26)
        let from = reader.ReadBytes(26)
        let nonce = reader.ReadInt64()
        let userAgent = reader.ReadVarString()
        let height = reader.ReadInt32()
        let relay = if version >= 70001 && reader.BaseStream.Position < reader.BaseStream.Length then reader.ReadByte() else 1uy
        new Version(version, services, timestamp, recv, from, nonce, userAgent, height, relay)

    override x.ToString() = sprintf "%s" userAgent
    member val Recv = recv with get
    member val From = from with get
    member val UserAgent = userAgent with get
    member val Relay = relay with get
    member val Height = height with get

(*** hide ***)
type AddrEntry = {
    Timestamp: int32
    Address: NetworkAddr
    }
type Addr(addrs: AddrEntry[]) =
    member x.ToByteArray() = ToBinaryArray (fun os ->
        os.WriteVarInt(addrs.Length)
        for addr in addrs do
            os.Write(addr.Timestamp)
            os.Write(addr.Address.ToByteArray())
    )
    static member Parse(reader: BinaryReader) =
        let count = reader.ReadVarInt()
        let addrs = 
            seq {
                for _ in 1..count do
                    let timestamp = reader.ReadInt32()
                    let nodeAddr = NetworkAddr.Parse(reader)
                    yield { Timestamp = timestamp; Address = nodeAddr }
                } |> Seq.toArray
        new Addr(addrs)
    member val Addrs = addrs with get

type GetHeaders(hashes: byte[] list, hashStop: byte[]) =
    member x.ToByteArray() = ToBinaryArray (fun os ->
        os.Write(version)
        os.WriteVarInt(Seq.length hashes)
        for h in hashes do 
            os.Write(h)
        os.Write(hashStop)
        )

    static member Parse(reader: BinaryReader) = 
        let version = reader.ReadInt32()
        let hashCount = int(reader.ReadVarInt())
        let hashes = 
            seq { 
                for _ in 1..hashCount do
                let hash = reader.ReadBytes(32)
                yield hash
            } |> Seq.toList
        let hashStop = reader.ReadBytes(32)
        new GetHeaders(hashes, hashStop)

    override x.ToString() = sprintf "GetHeaders(%s)" (hashToHex(Seq.head hashes))
    member val Hashes = hashes with get
    member val HashStop = hashStop with get

type BlockHeaderCompare() =
    let hashCompare = new HashCompare() :> IEqualityComparer<byte[]>
    interface IEqualityComparer<BlockHeader> with
        member x.Equals(left: BlockHeader, right: BlockHeader) =
            if obj.ReferenceEquals(left, right) then true
            else hashCompare.Equals(left.Hash, right.Hash)

        member x.GetHashCode(header: BlockHeader) = hashCompare.GetHashCode(header.Hash)

and BlockHeader(hash: byte[], version: int, prevBlockHash: byte[], merkleRoot: byte[], timestamp: uint32, bits: int, nonce: int, txCount: int) =
    let mutable height = 0
    let mutable nextBlockHash: byte[] = Array.empty
    let mutable isMain = false
    new(hash: byte[]) = BlockHeader(hash, 0, Array.zeroCreate 32, Array.zeroCreate 32, 0u, 0, 0, 0)
    member x.ToByteArray (nocount: bool) (full: bool) = ToBinaryArray (fun os ->
        os.Write(version)
        os.Write(prevBlockHash)
        os.Write(merkleRoot)
        os.Write(timestamp)
        os.Write(bits)
        os.Write(nonce)
        if not nocount then os.WriteVarInt(if full then txCount else 0)
    )
    static member Parse(reader: BinaryReader) =
        let headerHashPart = reader.ReadBytes(80)
        let hash = dsha headerHashPart
        let txCount = reader.ReadVarInt()
        ParseByteArray headerHashPart (fun r ->
            let version = r.ReadInt32()
            let prevBlockHash = r.ReadBytes(32)
            let merkleRoot = r.ReadBytes(32)
            let timestamp = r.ReadUInt32()
            let bits = r.ReadInt32()
            let nonce = r.ReadInt32()
            new BlockHeader(hash, version, prevBlockHash, merkleRoot, timestamp, bits, nonce, txCount)
            )
    static member Zero = BlockHeader(Array.zeroCreate 32)
    override x.ToString() = sprintf "BlockHeader(%s, %d)" (hashToHex hash) (height)
    member val Hash = hash with get
    member val PrevHash = prevBlockHash with get
    member val Version = version with get
    member val MerkleRoot = merkleRoot with get
    member val Timestamp = timestamp with get
    member val Bits = bits with get
    member val Nonce = nonce with get
    member val TxCount = txCount with get
    member x.Height with get() = height and set value = height <- value
    member x.NextHash with get() = nextBlockHash and set value = nextBlockHash <- value
    member x.IsMain with get() = isMain and set value = isMain <- value

[<AllowNullLiteral>]
type Headers(headers: BlockHeader list) =
    member x.ToByteArray() = ToBinaryArray (fun os ->
        os.WriteVarInt(headers.Length)
        for header in headers do
            os.Write(header.ToByteArray false false)
    )
    static member Parse(reader: BinaryReader) =
        let count = reader.ReadVarInt()
        let headers = 
            seq { 
            for _ in 1..count do
                yield BlockHeader.Parse(reader)
            } |> Seq.toList
            
        new Headers(headers)
    override x.ToString() = sprintf "Headers(%A)" headers
    member val Headers = headers with get

type InvEntry(tpe: int, hash: byte[]) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(tpe)
        os.Write(hash)
        )
    member val Type = tpe with get
    member val Hash = hash with get
    override x.ToString() = sprintf "InvEntry(%A)" hash
    static member Parse(reader: BinaryReader) =
        let tpe = reader.ReadInt32()
        let hash = reader.ReadBytes(32)
        new InvEntry(tpe, hash)

type InvVector(invs: InvEntry list) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.WriteVarInt(invs.Length)
        for inv in invs do
            os.Write(inv.ToByteArray())
        )
    member val Invs = invs with get
    static member Parse(reader: BinaryReader) =
        let count = reader.ReadVarInt()
        let invs = 
            seq {
            for _ in 1..count do
                yield InvEntry.Parse(reader)
            } |> Seq.toList
        new InvVector(invs)

type NotFound(invs: InvEntry list) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.WriteVarInt(invs.Length)
        for inv in invs do
            os.Write(inv.ToByteArray())
        )
    member val Invs = invs with get
    static member Parse(reader: BinaryReader) =
        let count = reader.ReadVarInt()
        let invs = 
            seq {
            for _ in 1..count do
                yield InvEntry.Parse(reader)
            } |> Seq.toList
        new NotFound(invs)

type GetData(invs: InvEntry list) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.WriteVarInt(invs.Length)
        for inv in invs do
            os.Write(inv.ToByteArray())
        )

    member val Invs = invs with get
    static member Parse(reader: BinaryReader) =
        let count = reader.ReadVarInt()
        let invs = 
            seq {
            for _ in 1..count do
                yield InvEntry.Parse(reader)
            } |> Seq.toList
        new GetData(invs)

type OutPoint(hash: byte[], index: int) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(hash)
        os.Write(index)
        )
    static member Parse(reader: BinaryReader) =
        let hash = reader.ReadBytes(32)
        let index = reader.ReadInt32()
        new OutPoint(hash, index)
    override x.ToString() = sprintf "OutPoint(%A,%d)" (hashToHex hash) index
    member val Hash = hash with get
    member val Index = index with get

type TxIn(prevOutpoint: OutPoint, script: byte[], sequence: int) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(prevOutpoint.ToByteArray())
        os.WriteScript(script)
        os.Write(sequence)
        )
        
    static member Parse(reader: BinaryReader) =
        let prevOutpoint = OutPoint.Parse(reader)
        let script = reader.ReadScript()
        let sequence = reader.ReadInt32()
        new TxIn(prevOutpoint, script, sequence)
    member val PrevOutPoint = prevOutpoint with get
    member val Script = script with get
    member val Sequence = sequence with get

type TxOut(value: int64, script: byte[]) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(value)
        os.WriteScript(script)
        )
    static member Parse(reader: BinaryReader) =
        let value = reader.ReadInt64()
        let script = reader.ReadScript()
        new TxOut(value, script)
    member val Value = value with get
    member val Script = script with get

// AllowNull because these will be put in a .NET Dictionary
[<AllowNullLiteral>]
type UTXO(txOut: TxOut, height: int) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(txOut.ToByteArray())
        os.Write(height)
        )
    static member Parse(reader: BinaryReader) =
        let txOut = TxOut.Parse(reader)
        let height = reader.ReadInt32()
        new UTXO(txOut, height)
    member val TxOut = txOut with get
    member val Height = height with get

// AllowNull because these will be put in a .NET Dictionary
[<AllowNullLiteral>]
type Tx(hash: byte[], version: int, txIns: TxIn[], txOuts: TxOut[], lockTime: uint32) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(version)
        os.WriteVarInt(txIns.Length)
        for txIn in txIns do
            os.Write(txIn.ToByteArray())
        os.WriteVarInt(txOuts.Length)
        for txOut in txOuts do
            os.Write(txOut.ToByteArray())
        os.Write(lockTime)
        )
    static member Parse(reader: BinaryReader) =
        let beginPosition = reader.BaseStream.Position
        let version = reader.ReadInt32()
        let txInCount = reader.ReadVarInt()
        let txIns = 
            seq {
            for _ in 1..txInCount do 
                yield TxIn.Parse(reader)
            }
            |> Seq.toArray
            
        let txOutCount = reader.ReadVarInt()
        let txOuts = 
            seq {
            for _ in 1..txOutCount do 
                yield TxOut.Parse(reader)
            }
            |> Seq.toArray
        let lockTime = reader.ReadUInt32()
        let endPosition = reader.BaseStream.Position
        let txSize = endPosition - beginPosition
        reader.BaseStream.Seek(beginPosition, SeekOrigin.Begin) |> ignore
        let txBytes = reader.ReadBytes(int txSize)
        let txHash = dsha txBytes
        new Tx(txHash, version, txIns, txOuts, lockTime)
    override x.ToString() = sprintf "%s" (hashToHex hash)
    member val Version = version with get
    member val Hash = hash with get
    member val TxIns = txIns with get
    member val TxOuts = txOuts with get
    member val LockTime = lockTime with get

type Block(header: BlockHeader, txs: Tx[]) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(header.ToByteArray false true)
        for tx in txs do
            os.Write(tx.ToByteArray())
    )
    static member Parse(reader: BinaryReader) =
        let header = BlockHeader.Parse(reader)
        let txs = 
            seq {
            for _ in 1..header.TxCount do
                yield Tx.Parse(reader)
            }
            |> Seq.toArray
        new Block(header, txs)
    member val Header = header with get
    member val Txs = txs with get
    
type MerkleBlock(header: BlockHeader, txHashes: byte[] list, flags: byte[]) =
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(header.ToByteArray true false)
        os.Write(header.TxCount)
        os.WriteVarInt(txHashes.Length)
        for txHash in txHashes do
            os.Write(txHash)
        os.WriteVarInt(flags.Length)
        os.Write(flags)
    )

    member val Header = header with get
    member val TxHashes = txHashes with get
    member val Flags = flags with get

type Mempool() = 
    member x.ToByteArray() = ToBinaryArray ignore
    static member Parse(reader: BinaryReader) =
        new Mempool()

type Ping(nonce: uint64) = 
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(nonce)
    )
    static member Parse(reader: BinaryReader) =
        let nonce = reader.ReadUInt64()
        new Ping(nonce)
    member val Nonce = nonce with get
type Pong(nonce: uint64) = 
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.Write(nonce)
    )
    static member Parse(reader: BinaryReader) =
        let nonce = reader.ReadUInt64()
        new Pong(nonce)
    member val Nonce = nonce with get

type GetBlocks(hashes: byte[] list, hashStop: byte[]) =
    member x.ToByteArray() = ToBinaryArray (fun os ->
        os.Write(version)
        os.WriteVarInt(Seq.length hashes)
        for h in hashes do 
            os.Write(h)
        os.Write(hashStop)
        )

    static member Parse(reader: BinaryReader) = 
        let version = reader.ReadInt32()
        let hashCount = int(reader.ReadVarInt())
        let hashes = 
            seq { 
                for _ in 1..hashCount do
                let hash = reader.ReadBytes(32)
                yield hash
            } |> Seq.toList
        let hashStop = reader.ReadBytes(32)
        new GetBlocks(hashes, hashStop)

    override x.ToString() = sprintf "GetBlocks(%s)" (hashToHex(Seq.head hashes))
    member val Hashes = hashes with get
    member val HashStop = hashStop with get

type FilterLoad(filter: byte[], nHash: int, nTweak: int, nFlags: byte) = 
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.WriteVarInt(filter.Length)
        os.Write(filter)
        os.Write(nHash)
        os.Write(nTweak)
        os.Write(nFlags)
    )
    static member Parse(reader: BinaryReader) =
        let filterLen = reader.ReadVarInt()
        let filter = reader.ReadBytes(filterLen)
        let nHash = reader.ReadInt32()
        let nTweak = reader.ReadInt32()
        let nFlags = reader.ReadByte()
        new FilterLoad(filter, nHash, nTweak, nFlags)
    member val Filter = filter with get
    member val NHash = nHash with get
    member val NTweak = nTweak with get
    member val NFlags = nFlags with get

type FilterAdd(data: byte[]) = 
    member x.ToByteArray() = ToBinaryArray(fun os ->
        os.WriteVarInt(data.Length)
        os.Write(data)
    )
    static member Parse(reader: BinaryReader) =
        let dataLen = reader.ReadVarInt()
        let data = reader.ReadBytes(dataLen)
        new FilterAdd(data)
    member val Data = data with get

type FilterClear() = 
    member x.ToByteArray() = Array.empty
    static member Parse(reader: BinaryReader) =
        new FilterClear()

(**
A `BitcoinMessage` is still a low-level object. The command has been identified and the payload
extracted but the later hasn't been parsed.
*)
let magic = if settings.TestNet then 0xDAB5BFFA else 0xD9B4BEF9 
type BitcoinMessage(command: string, payload: byte[]) =
    let parsePayload(): obj = 
        use ms = new MemoryStream(payload)
        match command with
        | "getheaders" -> ParseByteArray payload GetHeaders.Parse :> obj
        | "getblocks" -> ParseByteArray payload GetBlocks.Parse :> obj
        | "getdata" -> ParseByteArray payload GetData.Parse :> obj
        | "version" -> ParseByteArray payload Version.Parse :> obj
        | "headers" -> ParseByteArray payload Headers.Parse :> obj
        | "addr" -> ParseByteArray payload Addr.Parse :> obj
        | "block" -> ParseByteArray payload Block.Parse :> obj
        | "inv" -> ParseByteArray payload InvVector.Parse :> obj
        | "tx" -> ParseByteArray payload Tx.Parse :> obj
        | "ping" -> ParseByteArray payload Ping.Parse :> obj
        | "mempool" -> ParseByteArray payload Mempool.Parse :> obj
        | "filterload" -> ParseByteArray payload FilterLoad.Parse :> obj
        | "filteradd" -> ParseByteArray payload FilterAdd.Parse :> obj
        | "filterclear" -> ParseByteArray payload FilterClear.Parse :> obj
        | _ -> null

    member x.ToByteArray() = 
        let hash256 = dsha payload
        let checksum = hash256.[0..3]
        use ms = new MemoryStream()
        use os = new BinaryWriter(ms)
        os.Write(magic)
        let mutable c = Encoding.ASCII.GetBytes(command)
        Array.Resize(&c, 12) // Pads with null bytes
        os.Write(c)
        os.Write(payload.Length)
        os.Write(checksum)
        os.Write(payload)
        ms.ToArray()

    static member Parse(reader: BinaryReader) = 
        let magic = reader.ReadInt32()
        let command = (new string(reader.ReadChars(12))).TrimEnd([| '\000' |])
        let length = reader.ReadInt32()
        let checksum = reader.ReadInt32()
        let payload = reader.ReadBytes(length)

        new BitcoinMessage(command, payload)

    override x.ToString() = command
    member val Command = command with get
    member val Payload = payload with get
    member x.ParsePayload() = parsePayload()

(**
The message parser hooks up to an Observable of network buffers and produces
an Observable of `BitcoinMessage`. Network buffers can split messages at any point since
they are not aware of the semantics of the data transported. Therefore, there can be several
messages in one buffer and/or a message can be split accross multiple buffers.
The network buffers are treated as a sequence of byte arrays over which I fold a parser
function. It maintains a parsing state that has the type of the current message and the position
(inside the header part or the payload part) and it returns a sequence of messages.
Running the fold gives a sequence of sequence of messages so they need to be flatten to a simple sequence.
If the data is ill-formed, because of a bad magic value or an invalid checksum for example, the stream will raise
an `ParseException` that will be propagated as an Error element through the Observable. This method ensures that 
exceptions are treated as data and not as control flow.
*)
exception ParseException 

type ParserState = {
    InHeader: bool
    Command: string
    Checksum: int
    PayloadLength: int
    Data: byte[]
}

type BitcoinMessageParser(networkData: IObservable<byte[]>) =
    let headerLen = 24

    let rec parse (state: ParserState) (reader: BinaryReader) (messages: BitcoinMessage list) = 
        let remaining = int(reader.BaseStream.Length - reader.BaseStream.Position)
        if (state.InHeader && remaining < headerLen) || (not state.InHeader && remaining < state.PayloadLength) then
            (messages, state)
        else
            if state.InHeader then
                let messageMagic = reader.ReadInt32()
                if messageMagic <> magic then
                    raise ParseException
                else
                    let command = (new string(reader.ReadChars(12))).TrimEnd([| '\000' |])
                    let payloadLength = reader.ReadInt32()
                    let checksum = reader.ReadInt32()
                    parse 
                        ({ state with InHeader = false; Command = command; Checksum = checksum; PayloadLength = payloadLength })
                        reader
                        messages
            else 
                let payload = reader.ReadBytes(state.PayloadLength)
                let hash256 = dsha payload
                let checksum = BitConverter.ToInt32(hash256, 0)
                if checksum <> state.Checksum then
                    raise ParseException
                parse 
                    ({ state with InHeader = true })
                    reader
                    (new BitcoinMessage(state.Command, payload) :: messages)

    let acc (state: ParserState) (chunk: byte[]): BitcoinMessage list * ParserState =
        let data = Array.concat [state.Data; chunk]
        use ms = new MemoryStream(data)
        use reader = new BinaryReader(ms)
        let (messages, newState) = parse state reader List.empty
        let remainingBytes = data.[int ms.Position..]
        (messages, { newState with Data = remainingBytes })

    let bitcoinMessages = 
        networkData
            .Scan(
                (List.empty<BitcoinMessage>, { InHeader = true; Data = Array.empty; Command = ""; Checksum = 0; PayloadLength = 0}),
                fun (messages, state) (chunk) ->
                    acc state chunk 
            )
            .Select(fst)
            .SelectMany(fun m -> (m |> List.toSeq).ToObservable())
            // .Select(fun m -> logger.DebugF "Incoming %A" m; m)
    member x.BitcoinMessages with get() = bitcoinMessages

let hashCompare = new HashCompare() :> IEqualityComparer<byte[]>

let awaitTask (t: Task) = t |> Async.AwaitIAsyncResult |> Async.Ignore

let decodeAddress (address: IPAddress) = 
    match address.AddressFamily with
    | Sockets.AddressFamily.InterNetwork -> (1uy, address.GetAddressBytes())
    | Sockets.AddressFamily.InterNetworkV6 ->
        let addressBytes = address.GetAddressBytes()
        if settings.UseTor && addressBytes.[0..5] = torPrefix
        then
            let onionAddress = addressBytes.[6..]
            let onionUrl = sprintf "%s.onion" (Base32.Base32Encoder.Encode(onionAddress))
            logger.DebugF "Onion address %s" onionUrl
            let onionUrlBytes = ASCIIEncoding.ASCII.GetBytes(onionUrl)
            (3uy, Array.concat [[|byte onionUrlBytes.Length|]; onionUrlBytes])
        else (4uy, address.GetAddressBytes())

let connect(address: IPAddress) (port: int) = 
    let socksConnect() = 
        async {
            logger.DebugF "Connecting to %A" address
            let client = new Sockets.TcpClient()
            let host = IPAddress.Parse(settings.SocksHost)
            let socksEndPoint = new IPEndPoint(host, settings.SocksPort)
            do! awaitTask(client.ConnectAsync(host, settings.SocksPort))
            let versionMessage = [|5uy; 1uy; 0uy|]
            let stream = client.GetStream()
            let buffer: byte[] = Array.zeroCreate 1024
            do! stream.AsyncWrite(versionMessage, 0, versionMessage.Length)
            do! stream.AsyncRead(buffer, 0, buffer.Length) |> Async.Ignore
            let authMethod = buffer.[1]

            let connectMessage = ToBinaryArray(fun os ->
                os.Write(5uy)
                os.Write(1uy) // connect
                os.Write(0uy)
                let (addressType, addressBytes) = decodeAddress address
                os.Write(addressType)
                os.Write(addressBytes, 0, addressBytes.Length)
                os.Write(IPAddress.HostToNetworkOrder (int16 port))
                )
            do! stream.AsyncWrite(connectMessage, 0, connectMessage.Length)
            do! stream.AsyncRead(buffer, 0, buffer.Length) |> Async.Ignore
            if buffer.[1] <> 0uy then raise (IOException("SOCKS server returned error"))
            return stream
            }

    let normalConnect() = 
        async {
            let client = new Sockets.TcpClient()
            do! awaitTask(client.ConnectAsync(address, int port))
            let stream = client.GetStream()
            return stream
            }
    
    if settings.UseSocks then socksConnect() else normalConnect()
