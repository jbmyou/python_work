// This file contains your Data Connector logic
[Version = "1.0.0"]
section test;

[DataSource.Kind="test", Publish="test.Publish"]
shared test.Contents = (optional message as text) =>
    let
        _message = if (message <> null) then message else "(no message)",
        a = "Hello from test: " & _message
    in
        a;

// Data Source Kind description
test = [
    Authentication = [
        // Key = [],
        // UsernamePassword = [],
        // Windows = [],
        Anonymous = []
    ]
];

// Data Source UI publishing description
test.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = test.Icons,
    SourceTypeImage = test.Icons
];

test.Icons = [
    Icon16 = { Extension.Contents("test16.png"), Extension.Contents("test20.png"), Extension.Contents("test24.png"), Extension.Contents("test32.png") },
    Icon32 = { Extension.Contents("test32.png"), Extension.Contents("test40.png"), Extension.Contents("test48.png"), Extension.Contents("test64.png") }
];
