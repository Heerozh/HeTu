using System;
using UnityEditor;
using UnityEngine;

namespace HeTu.Editor.Setup
{
    public class HeTuPackageSetupWizard : EditorWindow
    {
        private void OnEnable() => EditorApplication.update += OnEditorUpdate;

        private void OnDisable() => EditorApplication.update -= OnEditorUpdate;

        private void OnGUI()
        {
            GUILayout.Space(8);

            EditorGUILayout.LabelField("Setup Wizard", EditorStyles.boldLabel);
            // 显示信息： 客户端SDK 需要安装依赖项，请手动安装。
            EditorGUILayout.HelpBox(
                "HeTu Client SDK requires the installation of the following dependencies. Please install them manually.",
                MessageType.Info);

            GUILayout.Space(8);

            DrawNuGetSection();

            GUILayout.Space(6);

            DrawUPMSection();

            GUILayout.Space(6);

            // DrawInstallRow(
            //     "R3 用于数据订阅的响应式编程支持。\n" +
            //     "R3  is used for reactive programming support for data subscriptions.",


            GUILayout.FlexibleSpace();

            using (new EditorGUILayout.HorizontalScope())
            {
                GUILayout.FlexibleSpace();
                if (GUILayout.Button("Close", GUILayout.Width(90)))
                    Close();
            }

            GUILayout.Space(8);
        }

        private void OnEditorUpdate()
        {
            if (UPMDependenciesInstaller.IsInstallInProgress)
                Repaint();
        }

        private static void DrawNuGetSection()
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField("1. NuGet Dependencies",
                    EditorStyles.boldLabel);
                EditorGUILayout.LabelField(
                    "需要首先安装 NuGet 相关依赖。\nFirst, ensure NuGet and dependencies are installed in your project.",
                    EditorStyles.wordWrappedMiniLabel);

                GUILayout.Space(4);

                foreach (var dep in NuGetDependenciesInstaller.Dependencies)
                {
                    DrawPackageInstallItem(
                        $"{dep.packageId} (>= {dep.minVersion})",
                        GetNuGetDescription(dep.packageId),
                        NuGetDependenciesInstaller.IsDependencyInstalled(
                            dep.packageId),
                        () =>
                        {
                            if (!EnsureNuGetForUnityInstalled())
                                return;

                            NuGetDependenciesInstaller.InstallDependency(
                                dep.packageId);
                        });
                }
            }
        }

        private static void DrawUPMSection()
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField("2. UPM Dependencies",
                    EditorStyles.boldLabel);
                EditorGUILayout.LabelField(
                    "然后安装UPM依赖。\nThen install UPM dependencies.",
                    EditorStyles.wordWrappedMiniLabel);

                GUILayout.Space(4);

                var canInstallUPM = NuGetDependenciesInstaller
                    .IsAllDependenciesInstalled();
                var upmInstalling = UPMDependenciesInstaller.IsInstallInProgress;

                if (upmInstalling)
                {
                    var current = UPMDependenciesInstaller.CurrentInstallingPackageId;
                    EditorGUILayout.HelpBox(
                        $"Installing package in background: {current}\nPlease wait until it completes.",
                        MessageType.Info);

                    var progress = Mathf.PingPong(
                        (float)EditorApplication.timeSinceStartup * 0.6f, 1f);
                    var rect = GUILayoutUtility.GetRect(18, 18,
                        GUILayout.ExpandWidth(true));
                    EditorGUI.ProgressBar(rect, progress,
                        "Installing...");
                    GUILayout.Space(4);
                }

                foreach (var dep in UPMDependenciesInstaller.Dependencies)
                {
                    var installed =
                        UPMDependenciesInstaller.IsDependencyInstalled(dep.packageId);
                    var installing =
                        UPMDependenciesInstaller.IsDependencyInstalling(dep.packageId);

                    DrawPackageInstallItem(
                        dep.packageId,
                        GetUPMDescription(dep.packageId),
                        installed,
                        () =>
                        {
                            if (!canInstallUPM)
                            {
                                EditorUtility.DisplayDialog("Setup Failed",
                                    "Please install NuGet dependencies first.",
                                    "OK");
                                return;
                            }

                            UPMDependenciesInstaller.InstallDependency(dep.packageId);
                        },
                        !canInstallUPM || upmInstalling,
                        installing);
                }
            }
        }

        private static bool EnsureNuGetForUnityInstalled()
        {
            if (UPMDependenciesInstaller.IsUPMPackageInstalled(
                    "com.github-glitchenzo.nugetforunity"))
                return true;

            var (nuget, url) = ("com.github-glitchenzo.nugetforunity",
                "https://github.com/GlitchEnzo/NuGetForUnity.git?path=/src/NuGetForUnity");
            UPMDependenciesInstaller.InstallUPMPackage(nuget, url);
            EditorUtility.DisplayDialog("Setup Info",
                "正在安装 NuGet 包管理器，请完成后重新点击安装 NuGet 依赖。\n\n" +
                "NuGet Package Manager is being installed. Please click to install NuGet dependencies again after completion.",
                "OK");
            return false;
        }

        private static string GetNuGetDescription(string packageId)
        {
            switch (packageId)
            {
                case "MessagePack":
                    return "用于高效的序列化和反序列化 / Efficient serialization and deserialization.";
                case "BouncyCastle.Cryptography":
                    return "用于加密操作 / Cryptographic operations.";
                case "R3":
                    return "用于响应式编程支持 / Reactive programming support.";
                default:
                    return "NuGet dependency.";
            }
        }

        private static string GetUPMDescription(string packageId) =>
            packageId switch
            {
                "com.cysharp.unitask" =>
                    "用于异步编程支持（Unity 6 不安装）/ Async programming support (not installed on Unity 6).",
                "com.github.messagepack-csharp" =>
                    "用于 MessagePack 的 Unity 集成 / MessagePack Unity integration.",
                "com.cysharp.r3" => "响应式编程Unity扩展 / Reactive programming Unity support.",
                _ => "UPM dependency."
            };

        [MenuItem("HeTu/Setup Wizard...")]
        public static void Open()
        {
            var w = GetWindow<HeTuPackageSetupWizard>(true, "HeTu Setup");
            w.minSize = new Vector2(420, 560);
            w.Show();
        }

        [InitializeOnLoadMethod]
        private static void AutoPromptOncePerSession() =>
            // 可选：首次打开项目时自动弹一次。这里做“每次打开Unity都会弹”的最简版。
            // 如果你想“只弹一次”，加一个 EditorPrefs key 记录已提示过即可。
            EditorApplication.delayCall += () =>
            {
                // 如果都装完了就不弹
                if (UPMDependenciesInstaller.IsAllDependenciesInstalled() &&
                    NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    return;

                // 避免在编译/播放模式下打扰
                if (EditorApplication.isCompiling ||
                    EditorApplication.isPlayingOrWillChangePlaymode)
                    return;

                Open();
            };

        private static void DrawPackageInstallItem(string title, string description,
            bool installed, Action installAction, bool disableByPrerequisite = false,
            bool installing = false)
        {
            using (new EditorGUILayout.VerticalScope(EditorStyles.helpBox))
            {
                EditorGUILayout.LabelField(title, EditorStyles.miniBoldLabel);
                EditorGUILayout.LabelField(description,
                    EditorStyles.wordWrappedMiniLabel);

                GUILayout.Space(4);

                using (new EditorGUILayout.HorizontalScope())
                {
                    GUILayout.FlexibleSpace();

                    using (new EditorGUI.DisabledScope(
                               installed || disableByPrerequisite || installing))
                    {
                        var buttonText = installed
                            ? "Installed"
                            : installing
                                ? "Installing..."
                                : "Install";
                        if (GUILayout.Button(buttonText, GUILayout.Width(120)))
                        {
                            try
                            {
                                installAction?.Invoke();
                            }
                            catch (Exception e)
                            {
                                Debug.LogException(e);
                                EditorUtility.DisplayDialog("Setup Failed", e.Message,
                                    "OK");
                            }
                        }
                    }
                }
            }
        }
    }
}
