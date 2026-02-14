using System;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
#if NUGET_INSTALLED
using NugetForUnity;
using NugetForUnity.Models;
#endif

namespace HeTu.Editor
{
    public static class NuGetDependenciesInstaller
    {
        private static readonly (string packageId, string minVersion)[] s_dependencies =
        {
            ("BouncyCastle.Cryptography", "2.6.2"), ("MessagePack", "3.1.4"), ("R3", "1.3.0")
        };

        public static IReadOnlyList<(string packageId, string minVersion)> Dependencies =>
            s_dependencies;

#if NUGET_INSTALLED

        public static bool IsAllDependenciesInstalled()
        {
            foreach (var dep in s_dependencies)
            {
                // 已安装且版本满足 >= MinVersion -> 不处理
                if (IsInstalled(dep.packageId, dep.minVersion))
                    continue;
                return false;
            }

            return true;
        }

        public static bool IsDependencyInstalled(string packageId)
        {
            var dep = s_dependencies.FirstOrDefault(d =>
                string.Equals(d.packageId, packageId,
                    StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrEmpty(dep.packageId))
                return false;

            return IsInstalled(dep.packageId, dep.minVersion);
        }

        private static bool IsInstalled(string packageId, string minVersion)
        {
            var installed = InstalledPackagesManager.InstalledPackages
                .FirstOrDefault(p =>
                    string.Equals(p.Id, packageId, StringComparison.OrdinalIgnoreCase));
            return installed != null &&
                   installed.PackageVersion >= new NugetPackageVersion(minVersion);
        }

        private static void InstallPackage(string packageId, string minVersion)
        {
            var required = new NugetPackageIdentifier(packageId, minVersion)
            {
                // 表示这是显式安装的（会写入 packages.config，并且一般更符合“我要求它必须有”的语义）
                IsManuallyInstalled = true
            };

            // 1) 已安装且版本满足 >= MinVersion -> 不处理
            if (IsInstalled(packageId, minVersion))
            {
                Debug.Log(
                    $"[NuGetForUnity] Already installed: {packageId} >= {minVersion}");
                return;
            }

            // 2) 未安装/版本不够 -> 安装
            Debug.Log($"[NuGetForUnity] Installing: {packageId} >= {minVersion} ...");

            // refreshAssets=true 会触发 AssetDatabase.Refresh，安装后马上生效，但可能会比较慢
            // isSlimRestoreInstall=false 表示会安装依赖（更符合常规“确保可用”）
            // allowUpdateForExplicitlyInstalled=true 表示如果已经显式安装了旧版本，允许升级
            var ok = NugetPackageInstaller.InstallIdentifier(
                required);

            if (ok)
                Debug.Log($"[NuGetForUnity] Installed: {packageId} >= {minVersion}");
            else
                Debug.LogError(
                    $"[NuGetForUnity] Failed to install: {packageId} >= {minVersion} (请打开 NuGetForUnity 窗口查看日志/源配置)");
        }

        public static void InstallAllDependencies()
        {
            foreach (var (packageId, minVersion) in s_dependencies)
                InstallPackage(packageId, minVersion);
        }

        public static void InstallDependency(string packageId)
        {
            var dep = s_dependencies.FirstOrDefault(d =>
                string.Equals(d.packageId, packageId,
                    StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrEmpty(dep.packageId))
                throw new ArgumentException($"Unknown NuGet dependency: {packageId}",
                    nameof(packageId));

            InstallPackage(dep.packageId, dep.minVersion);
        }


#else
        public static bool IsAllDependenciesInstalled()
        {
            return false;
        }

        public static bool IsDependencyInstalled(string packageId)
        {
            return false;
        }

        public static void InstallAllDependencies()
        {

        }

        public static void InstallDependency(string packageId)
        {

        }
#endif
    }
}
