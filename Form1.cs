// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.Form1
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using AForge.Video;
using AForge.Video.DirectShow;
using System;
using System.Collections;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;

namespace WARPSPDIntegration
{
  public class Form1 : Form
  {
    private FilterInfoCollection VideoCaptureDevices;
    private VideoCaptureDevice FinalVideo;
    private IContainer components = (IContainer) null;
    private ComboBox comboBox1;
    private PictureBox pictureBox1;
    private Button button1;
    private Button button2;

    public Form1()
    {
      this.InitializeComponent();
      this.VideoCaptureDevices = new FilterInfoCollection(FilterCategory.VideoInputDevice);
      foreach (FilterInfo videoCaptureDevice in (CollectionBase) this.VideoCaptureDevices)
        this.comboBox1.Items.Add((object) videoCaptureDevice.Name);
      this.comboBox1.SelectedIndex = 0;
    }

    private void button1_Click(object sender, EventArgs e)
    {
      this.FinalVideo = new VideoCaptureDevice(this.VideoCaptureDevices[this.comboBox1.SelectedIndex].MonikerString);
      // ISSUE: method pointer
      this.FinalVideo.NewFrame += new NewFrameEventHandler((object) this, __methodptr(FinalVideo_NewFrame));
      this.FinalVideo.Start();
    }

    private void FinalVideo_NewFrame(object sender, NewFrameEventArgs eventArgs) => this.pictureBox1.Image = (Image) eventArgs.Frame.Clone();

    private void button2_Click(object sender, EventArgs e)
    {
      this.pictureBox1.Image.Save(Application.StartupPath + "\\ABC.JPG");
      this.FinalVideo.Stop();
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing && this.components != null)
        this.components.Dispose();
      base.Dispose(disposing);
    }

    private void InitializeComponent()
    {
      this.comboBox1 = new ComboBox();
      this.pictureBox1 = new PictureBox();
      this.button1 = new Button();
      this.button2 = new Button();
      ((ISupportInitialize) this.pictureBox1).BeginInit();
      this.SuspendLayout();
      this.comboBox1.FormattingEnabled = true;
      this.comboBox1.Location = new Point(234, 59);
      this.comboBox1.Name = "comboBox1";
      this.comboBox1.Size = new Size(327, 21);
      this.comboBox1.TabIndex = 0;
      this.pictureBox1.Location = new Point(234, 86);
      this.pictureBox1.Name = "pictureBox1";
      this.pictureBox1.Size = new Size(327, 206);
      this.pictureBox1.TabIndex = 1;
      this.pictureBox1.TabStop = false;
      this.button1.Location = new Point(234, 321);
      this.button1.Name = "button1";
      this.button1.Size = new Size(137, 33);
      this.button1.TabIndex = 2;
      this.button1.Text = "button1";
      this.button1.UseVisualStyleBackColor = true;
      this.button1.Click += new EventHandler(this.button1_Click);
      this.button2.Location = new Point(396, 321);
      this.button2.Name = "button2";
      this.button2.Size = new Size(137, 33);
      this.button2.TabIndex = 2;
      this.button2.Text = "button1";
      this.button2.UseVisualStyleBackColor = true;
      this.button2.Click += new EventHandler(this.button2_Click);
      this.AutoScaleDimensions = new SizeF(6f, 13f);
      this.AutoScaleMode = AutoScaleMode.Font;
      this.ClientSize = new Size(800, 450);
      this.Controls.Add((Control) this.button2);
      this.Controls.Add((Control) this.button1);
      this.Controls.Add((Control) this.pictureBox1);
      this.Controls.Add((Control) this.comboBox1);
      this.Name = nameof (Form1);
      this.Text = nameof (Form1);
      ((ISupportInitialize) this.pictureBox1).EndInit();
      this.ResumeLayout(false);
    }
  }
}
