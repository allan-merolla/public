$json= get-content "message.json" -raw | ConvertFrom-Json -Depth 32
$json.features | % {$_.id= $_.properties.lid}
(ConvertTo-Json $json -Depth 32).ToString() -replace " ","" -replace "`n","" -replace "`r","" | set-content -Path "message_fixed.json" -NoNewline








