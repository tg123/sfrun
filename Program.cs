using System;
using System.Fabric;
using System.Fabric.Description;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.Serialization;
using CommandLine;

namespace sfrun
{
    class Program
    {
        // https://stackoverflow.com/questions/58744/copy-the-entire-contents-of-a-directory-in-c-sharp
        public static void CopyFilesRecursively(DirectoryInfo source, DirectoryInfo target)
        {
            foreach (var dir in source.GetDirectories())
            {
                CopyFilesRecursively(dir, target.CreateSubdirectory(dir.Name));
            }

            foreach (var file in source.GetFiles())
            {
                file.CopyTo(Path.Combine(target.FullName, file.Name));
            }
        }

        public static long MaxTs(DirectoryInfo dir)
        {
            var max = 0L;

            void Walk(DirectoryInfo source)
            {
                foreach (var d in source.GetDirectories())
                {
                    Walk(d);
                }

                foreach (var file in source.GetFiles())
                {
                    var ts = new DateTimeOffset(file.LastWriteTimeUtc).ToUnixTimeMilliseconds();
                    max = Math.Max(ts, max);
                }
            }

            Walk(dir);

            return max;
        }


        class Options
        {
            [Value(0, HelpText = "exe file", Required = true)] public string ExeFile { get; set; }

            [Option("sfendpoint", HelpText = "Service Fabric Endpoint")] public string SfEndpoint { get; set; }

            [Option("port", HelpText = "Port should open to public")] public int? Port { get; set; }

            [Option("app", HelpText = "App name on SF")] public string AppName { get; set; }
        }

        static async Task Main(string[] args)
        {
            await Parser.Default.ParseArguments<Options>(args)
                .WithParsedAsync(Run);
        }

        private static async Task Run(Options arg)
        {
            var fabricClient = string.IsNullOrEmpty(arg.SfEndpoint)
                ? new FabricClient()
                : new FabricClient(arg.SfEndpoint);

            var fi = new FileInfo(arg.ExeFile);

            if (!fi.Exists)
            {
                Console.WriteLine("exe file not exists");
                return;
            }

            if (fi.Directory?.Exists != true)
            {
                Console.WriteLine("exe folder not exists");
                return;
            }

            var folder = fi.Directory.FullName;
            var main = fi.Name;
            var port = new int[0];

            if (arg.Port != null)
            {
                port = new[] {arg.Port.Value};
            }

            var app = "";

            foreach(var a in new []{arg.AppName, fi.Directory.Name, fi.Name})
            {
                if (!string.IsNullOrWhiteSpace(a))
                {
                    app = a;
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(app))
            {
                Console.WriteLine("Cannot generate app name");
                return;
            }

            app = app.First().ToString().ToUpper() + app.Substring(1);

            var maxts = MaxTs(fi.Directory);

            

            var version0 = new Version(1, 0, (int)(maxts >> 32) ,(int)maxts & int.MaxValue);
            var upgrade = false;


            var oldapp = (await fabricClient.QueryManager.GetApplicationListAsync(new Uri($"fabric:/{app}")))
                .FirstOrDefault();

            if (oldapp != null)
            {
                var ver = new Version(oldapp.ApplicationTypeVersion);

                if (ver >= version0)
                {
                    Console.WriteLine("Running app package is newer or same");
                    return;
                }

                upgrade = true;
            }

            var version = version0.ToString();


            Console.WriteLine($"Generating {app} with {version}");

            using (var path0 = new TempDirectory())
            {
                var path = path0.FullPath;

                var pkgpath = Path.Combine(path, $"{app}SFServicePkg");

                var codepath = Path.Combine(pkgpath, "Code");
                var configpath = Path.Combine(pkgpath, "Config");

                Directory.CreateDirectory(codepath);
                Directory.CreateDirectory(configpath);

                CopyFilesRecursively(new DirectoryInfo(folder), new DirectoryInfo(codepath));


                using (var f = File.OpenWrite(Path.Combine(path, "ApplicationManifest.xml")))
                {
                    var applicationManifest = GenerateApplicationManifest(app, version);
                    WriteToXml(applicationManifest, f);
                }

                using (var f = File.OpenWrite(Path.Combine(pkgpath, "ServiceManifest.xml")))
                {
                    var serviceManifest = GenerateServiceManifest(app, version, main, "", port);
                    WriteToXml(serviceManifest, f);
                }

                fabricClient.ApplicationManager.CopyApplicationPackage(
                    new FabricClientHelper(fabricClient).ImageStoreConnectionString, path, app);
                try
                {
                    await fabricClient.ApplicationManager.ProvisionApplicationAsync(app).ConfigureAwait(false);
                }
                catch (FabricElementAlreadyExistsException)
                {
                }

                try
                {
                    if (upgrade)
                    {
                        Console.WriteLine($"Updating to {version}");
                        await fabricClient.ApplicationManager.UpgradeApplicationAsync(
                            new ApplicationUpgradeDescription()
                            {
                                ApplicationName = new Uri($"fabric:/{app}"),
                                TargetApplicationTypeVersion = version,
                                UpgradePolicyDescription = new MonitoredRollingApplicationUpgradePolicyDescription()
                                {
                                    UpgradeMode = RollingUpgradeMode.Monitored,
                                    MonitoringPolicy = new RollingUpgradeMonitoringPolicy()
                                    {
                                        FailureAction = UpgradeFailureAction.Rollback
                                    }
                                },
                            }).ConfigureAwait(false);
                    }
                    else
                    {
                        Console.WriteLine($"Creating app {app}");
                        await fabricClient.ApplicationManager.CreateApplicationAsync(new ApplicationDescription
                        {
                            ApplicationName = new Uri($"fabric:/{app}"),
                            ApplicationTypeName = $"{app}SFApp",
                            ApplicationTypeVersion = version, // should align with provider
                        }).ConfigureAwait(false);
                    }
                }
                catch (FabricException e)
                {
                    if (e.ErrorCode != FabricErrorCode.ApplicationAlreadyExists)
                    {
                        throw;
                    }
                }
            }
        }

        public class FabricClientHelper
        {
            public string ImageStoreConnectionString { get; }

            public FabricClientHelper(FabricClient fabricClient)
            {
                var cm = XElement.Parse(fabricClient.ClusterManager.GetClusterManifestAsync().GetAwaiter().GetResult());
                ImageStoreConnectionString =
                    cm.Descendants().FirstOrDefault(e => e.Attribute("Name")?.Value == "ImageStoreConnectionString")
                        ?.Attribute("Value")?.Value ?? "fabric:ImageStore";
            }
        }

        private static ServiceManifestType GenerateServiceManifest(string app, string version, string main, string argv,
            int[] ports)
        {
            var serviceManifest = new ServiceManifestType()
            {
                Name = $"{app}SFServicePkg",
                Version = version
            };

            serviceManifest.ServiceTypes = new[]
            {
                new StatelessServiceTypeType()
                {
                    ServiceTypeName = $"{app}SFServiceType",
                    UseImplicitHost = true,
                }
            };

            serviceManifest.CodePackage = new[]
            {
                new CodePackageType()
                {
                    Name = "Code",
                    Version = version,
                    EntryPoint = new EntryPointDescriptionType()
                    {
                        Item = new EntryPointDescriptionTypeExeHost()
                        {
                            Program = main,
                            Arguments = argv,
                            WorkingFolder = ExeHostEntryPointTypeWorkingFolder.CodePackage
                        }
                    }
                }
            };


            serviceManifest.Resources = new ResourcesType()
            {
                Endpoints = ports.Select((p, i) => new EndpointType()
                {
                    Name = $"{app}Port{i}",
                    Port = p,
                    Protocol = EndpointTypeProtocol.tcp,
                    PortSpecified = true,
                }).ToArray()
            };

            return serviceManifest;
        }

        private static void WriteToXml(object o, Stream f)
        {
            var xml = new XmlSerializer(o.GetType());
            xml.Serialize(f, o);
        }

        private static ApplicationManifestType GenerateApplicationManifest(string app, string version)
        {
            var applicationManifest = new ApplicationManifestType();

            applicationManifest.ApplicationTypeName = $"{app}SFApp";
            applicationManifest.ApplicationTypeVersion = version;

            applicationManifest.ServiceManifestImport = new[]
            {
                new ApplicationManifestTypeServiceManifestImport()
                {
                    ServiceManifestRef =
                        new ServiceManifestRefType()
                        {
                            ServiceManifestName = $"{app}SFServicePkg",
                            ServiceManifestVersion = version,
                        }
                }
            };


            applicationManifest.DefaultServices = new DefaultServicesType()
            {
                Items = new[]
                {
                    new DefaultServicesTypeService()
                    {
                        Name = $"{app}SFService",
                        Item =
                            new StatelessServiceType()
                            {
                                ServiceTypeName = $"{app}SFServiceType",
                                InstanceCount = "-1",
                                SingletonPartition = new ServiceTypeSingletonPartition()
                            }
                    }
                }
            };

            return applicationManifest;
        }
    }
}