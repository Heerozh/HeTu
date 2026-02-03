using UnityEditor;
using UnityEngine;

namespace HeTu.Editor.Setup
{
    public class HeTuPackageSetupWizard : EditorWindow
    {

        [MenuItem("HeTu/Setup Wizard...")]
        public static void Open()
        {
            var w = GetWindow<HeTuPackageSetupWizard>(utility: true, title: "HeTu Setup");
            w.minSize = new Vector2(420, 430);
            w.Show();
        }

        [InitializeOnLoadMethod]
        private static void AutoPromptOncePerSession()
        {
            // 可选：首次打开项目时自动弹一次。这里做“每次打开Unity都会弹”的最简版。
            // 如果你想“只弹一次”，加一个 EditorPrefs key 记录已提示过即可。
            EditorApplication.delayCall += () =>
            {
                // 如果都装完了就不弹
                if (UPMDependenciesInstaller.IsAllDependenciesInstalled() && NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    return;

                // 避免在编译/播放模式下打扰
                if (EditorApplication.isCompiling || EditorApplication.isPlayingOrWillChangePlaymode)
                    return;

                Open();
            };
        }


        private void OnGUI()
        {
            GUILayout.Space(8);

            EditorGUILayout.LabelField("Setup Wizard", EditorStyles.boldLabel);
            // 显示信息： 客户端SDK 需要安装依赖项，请手动安装。
            EditorGUILayout.HelpBox("HeTu Client SDK requires the installation of the following dependencies. Please install them manually.", MessageType.Info);

            GUILayout.Space(8);

            DrawInstallRow(
                title: "1. NuGet Dependencies",
                description: "需要首先安装 NuGet 相关依赖。\nFirst, ensure NuGet and dependencies are installed in your project.\n\nMessagePack 依赖用于高效的序列化和反序列化。\nMessagePack is used for efficient serialization and deserialization.\n\nBouncyCastle 依赖用于加密操作。\nBouncyCastle is used for cryptographic operations.",
                installed: NuGetDependenciesInstaller.IsAllDependenciesInstalled(),
                installAction: () =>
                {
                    NuGetDependenciesInstaller.InstallAllDependencies();
                });

            GUILayout.Space(6);

            DrawInstallRow(
                title: "2. UPM Dependencies",
                description: "然后安装UPM依赖。\nThen install UPM dependencies.\n\nUniTask 依赖用于异步编程支持（Unity6不安装）。\nUniTask is used for async programming support (Unity 6 will not install this).\n\nMessagePack-CSharp 依赖用于 MessagePack 的 Unity 集成。\nMessagePack-CSharp is used for MessagePack Unity integration.",
                installed: UPMDependenciesInstaller.IsAllDependenciesInstalled(),
                installAction: () =>
                {
                    if (!NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    {
                        EditorUtility.DisplayDialog("Setup Failed", "Please install NuGet dependencies first.", "OK");
                        return;
                    }
                    UPMDependenciesInstaller.InstallAllDependencies();
                });

            GUILayout.Space(6);

            DrawInstallRow(
                title: "Optional. ",
                description: "R3 用于数据订阅的响应式编程支持（推荐）(可选）。\nR3  is used for reactive programming support for data subscriptions (recommended) (optional).",
                installed: UPMDependenciesInstaller.IsAllOptionalInstalled(),
                installAction: () =>
                {
                    if (!NuGetDependenciesInstaller.IsAllDependenciesInstalled())
                    {
                        EditorUtility.DisplayDialog("Setup Failed", "Please install NuGet dependencies first.", "OK");
                        return;
                    }
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

        private static void DrawInstallRow(string title, string description, bool installed, System.Action installAction)
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField(title, EditorStyles.boldLabel);
                EditorGUILayout.LabelField(description, EditorStyles.wordWrappedMiniLabel);

                GUILayout.Space(4);

                using (new EditorGUILayout.HorizontalScope())
                {
                    GUILayout.FlexibleSpace();

                    using (new EditorGUI.DisabledScope(installed))
                    {
                        var buttonText = installed ? "Installed" : "Install";
                        if (GUILayout.Button(buttonText, GUILayout.Width(120)))
                        {
                            try
                            {
                                installAction?.Invoke();
                            }
                            catch (System.Exception e)
                            {
                                Debug.LogException(e);
                                EditorUtility.DisplayDialog("Setup Failed", e.Message, "OK");
                            }
                        }
                    }
                }
            }
        }
    }
}
