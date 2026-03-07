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
        private VisualElement _chatShell;
        private Button _loginBtn;

        private VisualElement _loginPanel;
        private Label _statusLabel;
        private TextField _urlField;
        private TextField _userIdField;
        private TextField _userNameField;

        public long UserId { get; private set; }
        public string UserName { get; private set; }

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

        /// <summary>Fired after a successful login. Args: userId, userName.</summary>
        public event Action<long, string> OnLoggedIn;

        private void OnLoginClicked()
        {
            _ = DoLoginAsync();
        }

        private async Task DoLoginAsync()
        {
            // ── Parse inputs ──
            var url = _urlField?.value?.Trim();
            if (string.IsNullOrEmpty(url))
            {
                SetStatus("Please enter a server URL.");
                return;
            }

            if (!long.TryParse(_userIdField?.value?.Trim(), out var userId))
            {
                SetStatus("User ID must be a number.");
                return;
            }

            var userName = _userNameField?.value?.Trim();
            if (string.IsNullOrEmpty(userName)) userName = $"guest_{userId}";

            // ── Disable UI while connecting ──
            _loginBtn.SetEnabled(false);
            SetStatus("Connecting...");

            try
            {
                HeTuClient.Instance.OnConnected += HandleConnected;

                // Connect() blocks until disconnect
                var result = await HeTuClient.Instance.Connect(url, "password123");

                if (result != null && result != "Canceled")
                    SetStatus($"Disconnected: {result}");
                else
                    SetStatus("Disconnected.");
            }
            catch (Exception ex)
            {
                SetStatus($"Connection error: {ex.Message}");
            }
            finally
            {
                HeTuClient.Instance.OnConnected -= HandleConnected;

                // ── Back to log in state ──
                _loginBtn.SetEnabled(true);
                _loginPanel.style.display = DisplayStyle.Flex;
                _chatShell.style.display = DisplayStyle.None;
            }

            return;

            async void HandleConnected()
            {
                try
                {
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
                    SetStatus($"Login failed: {ex.Message}");
                    HeTuClient.Instance.Close(); // Trigger Connect to return
                }
            }
        }

        private void SetStatus(string text)
        {
            if (_statusLabel != null) _statusLabel.text = text;
        }
    }
}