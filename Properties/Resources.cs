// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.Properties.Resources
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using System.CodeDom.Compiler;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Resources;
using System.Runtime.CompilerServices;

namespace WOWIntegration.Properties
{
  [GeneratedCode("System.Resources.Tools.StronglyTypedResourceBuilder", "16.0.0.0")]
  [DebuggerNonUserCode]
  [CompilerGenerated]
  internal class Resources
  {
    private static ResourceManager resourceMan;
    private static CultureInfo resourceCulture;

    internal Resources()
    {
    }

    [EditorBrowsable(EditorBrowsableState.Advanced)]
    internal static ResourceManager ResourceManager
    {
      get
      {
        if (WOWIntegration.Properties.Resources.resourceMan == null)
          WOWIntegration.Properties.Resources.resourceMan = new ResourceManager("WARPSPDIntegration.Properties.Resources", typeof (WOWIntegration.Properties.Resources).Assembly);
        return WOWIntegration.Properties.Resources.resourceMan;
      }
    }

    [EditorBrowsable(EditorBrowsableState.Advanced)]
    internal static CultureInfo Culture
    {
      get => WOWIntegration.Properties.Resources.resourceCulture;
      set => WOWIntegration.Properties.Resources.resourceCulture = value;
    }
  }
}
