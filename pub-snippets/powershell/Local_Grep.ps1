 get-childitem "C:\*" -recurse |  Where-Object { $_.LastWriteTime -lt (get-date).AddDays(-3)}  | select-string -pattern "logo"
 get-childitem . -recurse |  ? { $_.LastWriteTime -gt (get-date).AddDays(-3)}  | select-string -pattern "logo" | ft -groupby Path > c:\output.txt
