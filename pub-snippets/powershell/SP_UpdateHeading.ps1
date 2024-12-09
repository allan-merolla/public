[void][System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SharePoint")
[void][System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SharePoint.Publishing")

D:
CD D:\Temp

$output = @()
$services = [Microsoft.SharePoint.Administration.SPFarm]::Local.Services
$services | ?{$_ -is [Microsoft.SharePoint.Administration.SPWebService]} | Select -Expand WebApplications | Select -Expand Sites | %{
        $site = $_
        $urls = $_.AllWebs.Names
        write-host $_.AllWebs.Names
        $urls | %{
            $weburl = $_
            $web = $site.OpenWeb($_)
            write-host $web.Url
                $web.Navigation.Quicklaunch | % {
                    try{
                        if ($_.Properties["NodeType"] -eq "Heading")
                        {
                           $output += New-Object PSObject -Property @{Web=($weburl);NodeTitle=($_.Title);NodeUrl=($_.Url)}
                        }
                    }catch{}
                }
            $web.Dispose()
            }
}
$output | Export-CSV -NoTypeInformation "AllHeadings2013.csv"
$output = @()

