using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WinSCP;

public class Ftp
{
    SessionOptions sessionOptions = new SessionOptions
    {
        Protocol = Protocol.Ftp,
        HostName = Config.Data.GetKey("FA_FTP_IN"),//"example.com",
        UserName = "anonymous",
        Password = "anonymous",
        FtpSecure = FtpSecure.None,

        PortNumber = Convert.ToInt32(Config.Data.GetKey("FA_FTP_IN_PORT")),
        FtpMode = FtpMode.Passive,
        Timeout = new TimeSpan(0, 30, 0)
        //SshHostKeyFingerprint = "ssh-rsa 2048 xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx"
    };
    SessionOptions sessionOutOptions = new SessionOptions
    {
        Protocol = Protocol.Ftp,
        HostName = Config.Data.GetKey("FA_FTP_OUT"),//"example.com",
        UserName = "anonymous",
        Password = "anonymous",
        FtpSecure = FtpSecure.None,

        PortNumber = Convert.ToInt32(Config.Data.GetKey("FA_FTP_OUT_PORT")),
        FtpMode = FtpMode.Passive,
        Timeout = new TimeSpan(0, 30, 0)
        //SshHostKeyFingerprint = "ssh-rsa 2048 xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx"
    };
    public void download(string remotePath, string localPath)
    {
        using (Session session = new Session())
        {
            // Connect
            session.Open(sessionOptions);

            // Upload files
            TransferOptions transferOptions = new TransferOptions
            {
                TransferMode = TransferMode.Binary
            };
            //Note:file-file (remote-local)
            //var rs = session.GetFiles("/test1/test1/File_1.txt", "D:\\New folder\\File_1.txt", false, transferOptions);
            var rs = session.GetFiles(remotePath, localPath, false, transferOptions);
            //Note:folder-folder (remote-local)
            //var rs = session.GetFiles("/test1/test1/", "D:\\New folder", false, transferOptions);

        }
    }
    public void upload(string remotePath, string localPath)
    {
        var strMessage = "";
        //var rs = TransferOperationResult;
        using (Session session = new Session())
        {
            // Connect
            session.Open(sessionOptions);

            // Upload files
            TransferOptions transferOptions = new TransferOptions
            {
                TransferMode = TransferMode.Binary
            };
            //Note:file-file (remote-local)
            //var rs = session.GetFiles("/test1/test1/File_1.txt", "D:\\New folder\\File_1.txt", false, transferOptions);
            var rs = session.PutFiles(localPath,remotePath, true, transferOptions);

            //Note:folder-folder (remote-local)
            //var rs = session.GetFiles("/test1/test1/", "D:\\New folder", false, transferOptions);
            if (!rs.IsSuccess)
                strMessage = "FAIL to upload";
        }
        if (!string.IsNullOrEmpty(strMessage))
        {
            throw new Exception(strMessage);
        }
        
    }
    public void test()
    {
        using (Session session = new Session())
        {
            // Connect
            session.Open(sessionOptions);

            // Upload files
            TransferOptions transferOptions = new TransferOptions
            {
                TransferMode = TransferMode.Binary
            };
            //Note:file-file (remote-local)
            var rs =session.GetFiles("/test1/test1/File_1.txt", "D:\\New folder\\File_1.txt", false,transferOptions);
            //Note:folder-folder (remote-local)
            //var rs = session.GetFiles("/test1/test1/", "D:\\New folder", false, transferOptions);




            //TransferOperationResult transferResult;
            //transferResult = session.PutFiles(@"d:\toupload\*", "/home/user/", false, transferOptions);

            //// Throw on any error
            //transferResult.Check();

            //// Print results
            //foreach (TransferEventArgs transfer in transferResult.Transfers)
            //{
            //    Console.WriteLine("Upload of {0} succeeded", transfer.FileName);
            //}
        }
    }
}