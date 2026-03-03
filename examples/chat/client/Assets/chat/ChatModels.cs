namespace Chat
{
    public enum ChatFeedItemType
    {
        Chat,
        SystemJoin,
        SystemLeave,
        SystemInfo,
    }

    public sealed class ChatFeedItemVm
    {
        public long Id;
        public ChatFeedItemType Type;
        public long Owner;
        public string Author;
        public string Text;
        public string TimeText;
        public bool IsSelf;
        public long CreatedAtMs;
    }

    public sealed class MemberVm
    {
        public long Id;
        public long Owner;
        public string Name;
        public bool Online;
        public long LastSeenMs;
    }
}
