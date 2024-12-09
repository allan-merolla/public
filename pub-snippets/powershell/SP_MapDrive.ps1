#install-module azuread
#NOTE: in internet explorer must disable protected mode
import-module azuread

$Password = ConvertTo-SecureString "<password>" -AsPlainText -Force

$cred = new-object -typename System.Management.Automation.PSCredential -argumentlist "<email>", $password

Connect-AzureAD -TenantId "<tenant_id>" -Credential $cred

net use z: /delete

net use z: \\sitename.sharepoint.com@SSL\DavWWWRoot\sites\intranet