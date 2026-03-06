using System;
using System.Threading.Tasks;
using HeTu;
using UnityEngine;
using UnityEngine.UIElements;

namespace Chat
{
    /// <summary>
    ///     Minimal login screen — shows how few lines you need to connect with HeTu.
    /// </summary>
    [RequireComponent(typeof(UIDocument))]
    public sealed class LoginView : MonoBehaviour 
    {
        /// <summary>Fired after a successful login. Args: userId, userName.</summary>
        public event Action<long, string> OnLoggedIn;

        public long UserId { get; private set; }
        public string UserName { get; private set; }

        private VisualElement _loginPanel;
        private VisualElement _chatShell;
        private TextField _urlField;
        private TextField _userIdField;
        private TextField _userNameField;
        private Button _loginBtn;
        private Label _statusLabel;

        private void OnEnable()
        {
            var root = GetComponent<UIDocument>().rootVisualElement;

            _loginPanel = root.Q("login-panel");
            _chatShell = root.Q("chat-shell");
            _urlField = root.Q<TextField>("login-url");
            _userIdField = root.Q<TextField>("login-userid");
            _userNameField = root.Q<TextField>("login-username");
            _loginBtn = root.Q<Button>("login-btn");
            _statusLabel = root.Q<Label>("login-status");

            _loginBtn.clicked += OnLoginClicked;
        }

        private void OnDisable()
        {
            if (_loginBtn != null) _loginBtn.clicked -= OnLoginClicked;
        }

        private void OnLoginClicked() => _ = DoLoginAsync();

        private async Task DoLoginAsync()
        {
            // ── Parse inputs ──
            var url = _urlField?.value?.Trim();
            if (string.IsNullOrEmpty(url)) { SetStatus("Please enter a server URL."); return; }

            if (!long.TryParse(_userIdField?.value?.Trim(), out var userId))
            { SetStatus("User ID must be a number."); return; }

            var userName = _userNameField?.value?.Trim();
            if (string.IsNullOrEmpty(userName)) userName = $"guest_{userId}";

            // ── Disable UI while connecting ──
            _loginBtn.SetEnabled(false);
            SetStatus("Connecting...");

            try
            {
                // Step 1: register connected callback, then start connecting
                var connectedTcs = new TaskCompletionSource<bool>();
                void OnConnected() => connectedTcs.TrySetResult(true);
                HeTuClient.Instance.OnConnected += OnConnected;

                // fire-and-forget — Connect() blocks until disconnect
                _ = HeTuClient.Instance.Connect(url, "password123");

                // wait until the OnConnected event fires
                await connectedTcs.Task;
                HeTuClient.Instance.OnConnected -= OnConnected;

                // Step 2: login RPC
                SetStatus("Logging in...");
                await HeTuClient.Instance.CallSystem("user_login", userId, userName);

                // ── Success ── switch to chat view
                UserId = userId;
                UserName = userName;
                _loginPanel.style.display = DisplayStyle.None;
                _chatShell.style.display = DisplayStyle.Flex;
                OnLoggedIn?.Invoke(userId, userName);
            }
            catch (Exception ex)
            {
                SetStatus($"Failed: {ex.Message}");
                _loginBtn.SetEnabled(true);
            }
        }

        private void SetStatus(string text)
        {
            if (_statusLabel != null) _statusLabel.text = text;
        }
    }
}
