[void][System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SharePoint")

D:
CD D:\Temp

$WelcomePages = @()
$services = [Microsoft.SharePoint.Administration.SPFarm]::Local.Services
$services | ?{$_ -is [Microsoft.SharePoint.Administration.SPWebService]} | Select -Expand WebApplications | Select -Expand Sites | %{
    $_.AllWebs | %{
    
        $weburl = $_.url

		$WelcomePages += New-Object PSObject -Property @{Web=($weburl);WelcomePage=($_.RootFolder.WelcomePage)}

		$_.RootFolder.Files                    | ?{$_.Name -match "\.aspx$"} | Select @{name="url";expression={$weburl + "/" + $_.name}},@{name="layout";expression={$_.properties["PublishingPageLayout"]}}       
        $_.Lists | ?{!$_.Hidden} | %{$_.Items} | ?{$_.Name -match "\.aspx$"} | Select @{name="url";expression={$weburl + "/" + $_.Url }},@{name="layout";expression={$_.properties["PublishingPageLayout"]}}
		$_.Dispose()
    
    }
    $_.Dispose()
} | %{
    #Write-Host $_.url
    $_ } | Export-CSV -NoTypeInformation "AllPageLayouts2007.csv"

$WelcomePages | Export-CSV -NoTypeInformation "WelcomePages2007.csv"
$WelcomePages = @()

