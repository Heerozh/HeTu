using UnityEditor;
using UnityEditor.PackageManager;
using UnityEngine;
using System.Linq;
using System.IO;

namespace HeTu.Editor
{
    public class UPMDependenciesInstaller
    {
        // 定义 Git 依赖
        private static readonly (string, string)[] s_dependencies = new[]
        {
            ("com.github.messagepack-csharp", "https://github.com/MessagePack-CSharp/MessagePack-CSharp.git?path=src/MessagePack.UnityClient/Assets/Scripts/MessagePack"),
#if !UNITY_6000_0_OR_NEWER
            ("com.cysharp.unitask", "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask")
#endif
        };

        private static readonly (string, string)[] s_optionalDependencies = new[]
        {
            ("com.cysharp.r3", "https://github.com/Cysharp/R3.git?path=src/R3.Unity/Assets/R3.Unity")
        };

        public static bool IsAllDependenciesInstalled()
        {
            foreach (var (name, depUrl) in s_dependencies)
            {
                if (!IsUPMPackageInstalled(name))
                {
                    return false;
                }
            }
            return true;
        }

        public static void InstallAllDependencies()
        {
            foreach (var (name, depUrl) in s_dependencies)
            {
                InstallUPMPackage(name, depUrl);
            }
        }

        public static bool IsAllOptionalInstalled()
        {
            foreach (var (name, depUrl) in s_optionalDependencies)
            {
                if (!IsUPMPackageInstalled(name))
                {
                    return false;
                }
            }
            return true;
        }

        public static void InstallAllOptional()
        {
            foreach (var (name, depUrl) in s_optionalDependencies)
            {
                InstallUPMPackage(name, depUrl);
            }
        }

        static bool IsUPMPackageInstalled(string packageID)
        {
            // 读取 manifest.json (简单粗暴法，也可以用 Client.List 解析)
            var manifestPath = Path.Combine(Application.dataPath, "..", "Packages", "manifest.json");
            var manifestContent = File.ReadAllText(manifestPath);

            // 这里只是简单的字符串匹配，实际建议用 JSON 库解析 Package Name
            // 假设依赖包名为 com.some.package
            return manifestContent.Contains(packageID);
        }

        static void InstallUPMPackage(string packageID, string packageUrl)
        {
            if (!IsUPMPackageInstalled(packageID))
            {
                Debug.Log($"[Installer] Installing dependency: {packageUrl}");
                Client.Add(packageUrl);
            }
        }


    }

}
