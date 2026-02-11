using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using UnityEditor;
using UnityEditor.PackageManager;
using UnityEditor.PackageManager.Requests;
using UnityEngine;

namespace HeTu.Editor
{
    public class UPMDependenciesInstaller
    {
        // 定义 Git 依赖
        private static readonly (string packageId, string packageUrl)[] s_dependencies =
        {
#if !UNITY_6000_0_OR_NEWER
            ("com.cysharp.unitask",
                "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask"),
#endif
            ("com.github.messagepack-csharp",
                "https://github.com/MessagePack-CSharp/MessagePack-CSharp.git?path=src/MessagePack.UnityClient/Assets/Scripts/MessagePack")
        };

        public static IReadOnlyList<(string packageId, string packageUrl)> Dependencies =>
            s_dependencies;

        private static readonly (string, string)[] s_optionalDependencies =
        {
            ("com.cysharp.r3",
                "https://github.com/Cysharp/R3.git?path=src/R3.Unity/Assets/R3.Unity")
        };

        private static AddRequest s_lastRequest;
        private static string s_currentInstallingPackageId;

        public static bool IsInstallInProgress
        {
            get
            {
                UpdateRequestState();
                return s_lastRequest != null && !s_lastRequest.IsCompleted;
            }
        }

        public static string CurrentInstallingPackageId
        {
            get
            {
                UpdateRequestState();
                return s_currentInstallingPackageId;
            }
        }

        public static bool IsAllDependenciesInstalled()
        {
            foreach (var dep in s_dependencies)
            {
                if (!IsUPMPackageInstalled(dep.packageId))
                    return false;
            }

            return true;
        }

        public static void InstallAllDependencies()
        {
            foreach (var dep in s_dependencies)
                InstallUPMPackage(dep.packageId, dep.packageUrl);
        }

        public static bool IsDependencyInstalled(string packageID)
        {
            return IsUPMPackageInstalled(packageID);
        }

        public static bool IsDependencyInstalling(string packageID)
        {
            return IsInstallInProgress &&
                   string.Equals(s_currentInstallingPackageId, packageID,
                       StringComparison.OrdinalIgnoreCase);
        }

        public static void InstallDependency(string packageID)
        {
            foreach (var dep in s_dependencies)
            {
                if (!string.Equals(dep.packageId, packageID,
                        StringComparison.OrdinalIgnoreCase))
                    continue;

                InstallUPMPackage(dep.packageId, dep.packageUrl);
                return;
            }

            throw new ArgumentException($"Unknown UPM dependency: {packageID}",
                nameof(packageID));
        }

        public static bool IsAllOptionalInstalled()
        {
            foreach (var (name, depUrl) in s_optionalDependencies)
            {
                if (!IsUPMPackageInstalled(name))
                    return false;
            }

            return true;
        }

        public static void InstallAllOptional()
        {
            foreach (var (name, depUrl) in s_optionalDependencies)
                InstallUPMPackage(name, depUrl);
        }

        public static bool IsUPMPackageInstalled(string packageID)
        {
            // 读取 manifest.json (简单粗暴法，也可以用 Client.List 解析)
            var manifestPath = Path.Combine(Application.dataPath, "..", "Packages",
                "manifest.json");
            var manifestContent = File.ReadAllText(manifestPath);

            // 这里只是简单的字符串匹配，实际建议用 JSON 库解析 Package Name
            // 假设依赖包名为 com.some.package
            return manifestContent.Contains(packageID);
        }

        public static async void InstallUPMPackage(string packageID, string packageUrl)
        {
            if (IsUPMPackageInstalled(packageID))
                return;

            while (IsInstallInProgress)
                await Task.Delay(100);

            Debug.Log($"[Installer] Installing dependency: {packageUrl}");
            s_currentInstallingPackageId = packageID;
            s_lastRequest = Client.Add(packageUrl);
        }

        private static void UpdateRequestState()
        {
            if (s_lastRequest == null || !s_lastRequest.IsCompleted)
                return;

            if (s_lastRequest.Status == StatusCode.Failure)
                Debug.LogError(
                    $"[Installer] Failed to install dependency {s_currentInstallingPackageId}: {s_lastRequest.Error?.message}");
            else
                Debug.Log(
                    $"[Installer] Installed dependency: {s_currentInstallingPackageId}");

            s_lastRequest = null;
            s_currentInstallingPackageId = null;
            AssetDatabase.Refresh();
        }
    }
}
