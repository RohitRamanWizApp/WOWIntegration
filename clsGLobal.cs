// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.clsGLobal
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using AppMethods;
using FormClass;
using System;
using System.Data;
using System.Windows.Forms;

namespace WOWIntegration
{
  internal static class clsGLobal
  {
    public static Generic AppMain = new Generic();

    public static bool DefaultLogin(string cAppPath)
    {
      bool flag = clsGLobal.AppMain.SetConnection("sspldev", "TESTING_H1", "sa", "sspl@h199", true);
      if (flag)
      {
        ((Variables) clsGLobal.AppMain).dmethod.InitializeCommand();
        LoginForm loginForm = new LoginForm();
        ((BaseForm) loginForm)._AppMethod = (object) clsGLobal.AppMain;
        loginForm.AppMethod_Login = (object) clsGLobal.AppMain;
        loginForm.AppPath = (object) cAppPath;
        clsGLobal.AppMain.LOGIN_OpenTable("", "");
        ((Variables) clsGLobal.AppMain).WizAppPath = cAppPath;
        loginForm.Initialize_Controls();
        ((Control) loginForm.txtUserName).Text = "SUPER";
        ((Control) loginForm.txtUserName).Tag = (object) "0000000";
        DataRow[] dataRowArray = ((Variables) clsGLobal.AppMain).dset.Tables["tUsers"].Select("user_code='0000000'");
        if (dataRowArray.Length != 0)
          loginForm.txtPWD.Text = clsGLobal.AppMain.Encrypt(Convert.ToString(dataRowArray[0]["passwd"]));
        loginForm.FillLocation();
        flag = loginForm.FValidate(false);
      }
      return flag;
    }
  }
}
