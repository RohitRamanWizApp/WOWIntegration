// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.Program
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using System;
using System.Windows.Forms;

namespace WOWIntegration
{
  internal static class Program
  {
    [STAThread]
    private static void Main()
    {
      Application.EnableVisualStyles();
      Application.SetCompatibleTextRenderingDefault(false);
            GET_TOKEN GT = new GET_TOKEN();
            GT.Token(Application.StartupPath, "T2", "SUPER", "123", "");



      if (clsGLobal.DefaultLogin(Application.StartupPath))
        Application.Run(new Form());
      else
        Application.ExitThread();
    }
  }
}
