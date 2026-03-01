using HeTu;
using MessagePack;

namespace Chat
{
    
    [MessagePackObject]
    public class ChatMessage : IBaseComponent
    {
        [Key("id")] public long ID { get; set; }
        [Key("created_at_ms")] public long CreatedAtMs;
        [Key("kind")] public string Kind;
        [Key("name")] public string Name;
        [Key("owner")] public long Owner;
        [Key("text")] public string Text;
    }
    
    
    [MessagePackObject]
    public class OnlineUser : IBaseComponent
    {
        [Key("id")] public long ID { get; set; }
        [Key("last_seen_ms")] public long LastSeenMs;
        [Key("name")] public string Name;
        [Key("online")] public sbyte Online;
        [Key("owner")] public long Owner;
    }
    
}