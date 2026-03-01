using UnityEngine.UIElements;

namespace Chat
{
    public static class ChatRenderers
    {
        public static VisualElement CreateFeedItem(ChatFeedItemVm item)
        {
            return item.Type == ChatFeedItemType.Chat
                ? CreateMessageRow(item)
                : CreateEventRow(item);
        }

        public static VisualElement CreateMemberRow(MemberVm member)
        {
            var row = new VisualElement();
            row.AddToClassList("member-row");
            row.AddToClassList(member.Online ? "member--online" : "member--offline");

            var dot = new VisualElement();
            dot.AddToClassList("member-status-dot");
            row.Add(dot);

            var name = new Label(member.Name);
            name.AddToClassList("member-name");
            row.Add(name);
            return row;
        }

        private static VisualElement CreateMessageRow(ChatFeedItemVm item)
        {
            var row = new VisualElement();
            row.AddToClassList("message-row");
            row.AddToClassList(item.IsSelf ? "message--self" : "message--other");

            var avatar = new VisualElement();
            avatar.AddToClassList("avatar");
            avatar.AddToClassList(item.IsSelf ? "avatar--self" : "avatar--other");
            var avatarText = new Label(GetInitial(item.Author));
            avatarText.AddToClassList("avatar-text");
            avatar.Add(avatarText);

            var content = new VisualElement();
            content.AddToClassList("message-content");

            var meta = new VisualElement();
            meta.AddToClassList("message-meta");
            var author = new Label(item.Author);
            author.AddToClassList("message-author");
            var time = new Label(item.TimeText);
            time.AddToClassList("message-time");
            meta.Add(author);
            meta.Add(time);

            var text = new Label(item.Text);
            text.AddToClassList("message-bubble");

            content.Add(meta);
            content.Add(text);

            row.Add(avatar);
            row.Add(content);
            return row;
        }

        private static VisualElement CreateEventRow(ChatFeedItemVm item)
        {
            var row = new VisualElement();
            row.AddToClassList("event-row");
            switch (item.Type)
            {
                case ChatFeedItemType.SystemJoin:
                    row.AddToClassList("event--join");
                    break;
                case ChatFeedItemType.SystemLeave:
                    row.AddToClassList("event--leave");
                    break;
            }

            var text = new Label(item.Text);
            text.AddToClassList("event-text");
            row.Add(text);
            return row;
        }

        private static string GetInitial(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return "?";
            }

            return name.Trim().Substring(0, 1).ToUpperInvariant();
        }
    }
}
