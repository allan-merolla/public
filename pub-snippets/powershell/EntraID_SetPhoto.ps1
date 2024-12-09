Set-ADUser <user-email> -Replace @{thumbnailPhoto=([byte[]](Get-Content "C:\temp\photo.jpg" -Encoding byte))}
