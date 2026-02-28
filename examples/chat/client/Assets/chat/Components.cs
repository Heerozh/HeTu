using HeTu;
using MessagePack;

namespace chat
{
    
    [MessagePackObject]
    public class ChatMessage : IBaseComponent
    {
        [Key("id")] public long ID { get; set; }
        [Key("name")] public string Name;
        [Key("owner")] public long Owner;
        [Key("text")] public string Text;
    }
    
    
    [MessagePackObject]
    public class OnlineUser : IBaseComponent
    {
        [Key("id")] public long ID { get; set; }
        [Key("name")] public string Name;
        [Key("owner")] public long Owner;
    }
    
}