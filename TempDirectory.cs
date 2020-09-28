using System;
using System.IO;

namespace sfrun
{
    public class TempDirectory : IDisposable
    {
        public TempDirectory()
        {
            FullPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(FullPath);
        }

        public string FullPath { get; }

        public void Dispose()
        {
            try
            {
                Directory.Delete(FullPath, true);
            }
            catch
            {
            }
        }
    }
}