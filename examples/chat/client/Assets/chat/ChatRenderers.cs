using System;
using UnityEngine.UIElements;

namespace Chat
{
    /// <summary>
    ///     Pure UI renderers — build VisualElements from HeTu Components directly.
    /// </summary>
    public static class ChatRenderers
    {
        // ── Messages ─────────────────────────────────────────────

        public static VisualElement CreateFeedItem(ChatMessage msg, long selfUserId)
        {
            return IsSystemMessage(msg.Kind)
                ? CreateEventRow(msg)
                : CreateMessageRow(msg, msg.Owner == selfUserId);
        }

        private static VisualElement CreateMessageRow(ChatMessage msg, bool isSelf)
        {
            var row = new VisualElement();
            row.AddToClassList("message-row");
            row.AddToClassList(isSelf ? "message--self" : "message--other");

            var avatar = new VisualElement();
            avatar.AddToClassList("avatar");
            avatar.AddToClassList(isSelf ? "avatar--self" : "avatar--other");
            var avatarText = new Label(GetInitial(msg.Name));
            avatarText.AddToClassList("avatar-text");
            avatar.Add(avatarText);

            var content = new VisualElement();
            content.AddToClassList("message-content");

            var meta = new VisualElement();
            meta.AddToClassList("message-meta");
            var author = new Label(msg.Name);
            author.AddToClassList("message-author");
            var time = new Label(FormatTime(msg.CreatedAtMs));
            time.AddToClassList("message-time");
            meta.Add(author);
            meta.Add(time);

            var text = new Label(msg.Text);
            text.AddToClassList("message-bubble");

            content.Add(meta);
            content.Add(text);
            row.Add(avatar);
            row.Add(content);
            return row;
        }

        private static VisualElement CreateEventRow(ChatMessage msg)
        {
            var row = new VisualElement();
            row.AddToClassList("event-row");

            var lower = msg.Text?.ToLowerInvariant() ?? "";
            if (lower.Contains("joined the chat")) row.AddToClassList("event--join");
            else if (lower.Contains("left the chat")) row.AddToClassList("event--leave");

            var text = new Label(msg.Text);
            text.AddToClassList("event-text");
            row.Add(text);
            return row;
        }

        // ── Members ──────────────────────────────────────────────

        public static VisualElement CreateMemberRow(OnlineUser member)
        {
            var row = new VisualElement();
            row.AddToClassList("member-row");
            row.AddToClassList(member.Online != 0 ? "member--online" : "member--offline");

            var dot = new VisualElement();
            dot.AddToClassList("member-status-dot");
            row.Add(dot);

            var name = new Label(member.Name);
            name.AddToClassList("member-name");
            row.Add(name);
            return row;
        }

        // ── Helpers ──────────────────────────────────────────────

        private static bool IsSystemMessage(string kind) =>
            string.Equals(kind, "system", StringComparison.OrdinalIgnoreCase);

        private static string GetInitial(string name) =>
            string.IsNullOrWhiteSpace(name) ? "?" : name.Trim().Substring(0, 1).ToUpperInvariant();

        private static string FormatTime(long createdAtMs)
        {
            if (createdAtMs <= 0) return "--:--";
            try
            {
                return DateTimeOffset
                    .FromUnixTimeMilliseconds(createdAtMs)
                    .ToLocalTime()
                    .ToString("HH:mm");
            }
            catch { return "--:--"; }
        }
    }
}
