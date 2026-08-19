#define AppName "Zonn Node"
#define AppPublisher "Zonn"
#define AppExeName "zonn-node.exe"
#ifndef AppVersion
  #define AppVersion "0.1.2-dev"
#endif
#ifndef SourceDir
  #define SourceDir "..\bin"
#endif
#ifndef IconFile
  #define IconFile "..\zonn-node.ico"
#endif

[Setup]
AppId={{B72E0AA4-CE30-4F1A-A64D-D81455A59589}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={localappdata}\Programs\Zonn Node
DefaultGroupName=Zonn Node
DisableProgramGroupPage=yes
OutputDir=..\installer
OutputBaseFilename=Zonn-Node-Setup-Windows-x64
Compression=lzma2
SolidCompression=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
WizardStyle=modern
UninstallDisplayName=Zonn Node
SetupIconFile={#IconFile}
UninstallDisplayIcon={app}\{#AppExeName}

[Files]
Source: "{#SourceDir}\zonn-node.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourceDir}\inference-worker.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourceDir}\inference_worker.py"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Zonn Node"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\Zonn Node"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch Zonn Node"; Flags: nowait postinstall skipifsilent
