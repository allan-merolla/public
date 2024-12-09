Get-childitem -Include <folderName> -Recurse -force | {$_.PSIsContainer} | Remove-Item -Force –Recurse

