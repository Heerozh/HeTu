using System;
using UnityEditor;
using UnityEngine;

namespace HeTu.Editor.Setup
{
    public class HeTuPackageSetupWizard : EditorWindow
    {
        private void OnGUI()
        {
            GUILayout.Space(8);

            EditorGUILayout.LabelField("Setup Wizard", EditorStyles.boldLabel);
            // 显示信息： 客户端SDK 需要安装依赖项，请手动安装。
            EditorGUILayout.HelpBox(
                "HeTu Client SDK requires the installation of the following dependencies. Please install them manually.",
                MessageType.Info);

            GUILayout.Space(8);

            var btnText = "Install";

            DrawInstallRow(
                "1. NuGet Dependencies",
                "需要首先安装 NuGet 相关依赖。\n" +
                "First, ensure NuGet and dependencies are installed in your project.\n\n" +
                "MessagePack 依赖用于高效的序列化和反序列化。\n" +
                "MessagePack is used for efficient serialization and deserialization.\n\n" +
                "BouncyCastle 依赖用于加密操作。\n" +
                "BouncyCastle is used for cryptographic operations.",
                NuGetDependenciesInstaller.IsAllDependenciesInstalled(),
                btnText,
                () =>
                {
                    if (!UPMDependenciesInstaller.IsUPMPackageInstalled(
                            "com.github-glitchenzo.nugetforunity"))
                    {
                        var (nuget, url) = ("com.github-glitchenzo.nugetforunity",
                            "https://github.com/GlitchEnzo/NuGetForUnity.git?path=/src/NuGetForUnity");
                        UPMDependenciesInstaller.InstallUPMPackage(nuget, url);
                        EditorUtility.DisplayDialog("Setup Info",
                            "正在安装 NuGet 包管理器，请完成后重新点击安装 NuGet 依赖。\n\n" +
                            "NuGet Package Manager is being installed. Please click to install NuGet dependencies again after completion.",
                            "OK");
                        return;
                    }

                    NuGetDependenciesInstaller.InstallAllDependencies();
                });

            GUILayout.Space(6);

            DrawInstallRow(
                "2. UPM Dependencies",
                "然后安装UPM依赖。\nThen install UPM dependencies.\n\n" +
                "UniTask 依赖用于异步编程支持（Unity6不安装）。\n" +
                "UniTask is used for async programming support (Unity 6 will not install this).\n\n" +
                "MessagePack-CSharp 依赖用于 MessagePack 的 Unity 集成。\n" +
                "MessagePack-CSharp is used for MessagePack Unity integration.",
                UPMDependenciesInstaller.IsAllDependenciesInstalled(),
                btnText,
                () =>
                {
                    if (!NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    {
                        EditorUtility.DisplayDialog("Setup Failed",
                            "Please install NuGet dependencies first.", "OK");
                        return;
                    }

                    UPMDependenciesInstaller.InstallAllDependencies();
                });

            GUILayout.Space(6);

            DrawInstallRow(
                "Optional. ",
                "R3 用于数据订阅的响应式编程支持（推荐）(可选）。\n" +
                "R3  is used for reactive programming support for data subscriptions (recommended) (optional).",
                UPMDependenciesInstaller.IsAllOptionalInstalled() &&
                NuGetDependenciesInstaller.IsAllOptionalInstalled(),
                btnText,
                () =>
                {
                    if (!NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    {
                        EditorUtility.DisplayDialog("Setup Failed",
                            "Please install NuGet dependencies first.", "OK");
                        return;
                    }

                    NuGetDependenciesInstaller.InstallAllOptional();
                    UPMDependenciesInstaller.InstallAllOptional();
                });

            GUILayout.FlexibleSpace();

            using (new EditorGUILayout.HorizontalScope())
            {
                GUILayout.FlexibleSpace();
                if (GUILayout.Button("Close", GUILayout.Width(90)))
                    Close();
            }

            GUILayout.Space(8);
        }

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

        private static void DrawInstallRow(string title, string description,
            bool installed, string btnText, Action installAction)
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField(title, EditorStyles.boldLabel);
                EditorGUILayout.LabelField(description,
                    EditorStyles.wordWrappedMiniLabel);

                GUILayout.Space(4);

                using (new EditorGUILayout.HorizontalScope())
                {
                    GUILayout.FlexibleSpace();

                    using (new EditorGUI.DisabledScope(installed))
                    {
                        var buttonText = installed ? "Installed" : btnText;
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
