#    Load the SharePoint assemblies

[Void][System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SharePoint")

 

$site = New-Object Microsoft.SharePoint.SPSite("http://XXX")

 

$site.AllWebs | %{

    $web  = $_

    $web.Lists["Pages"].Items | ?{$_.File.Properties["PublishingPageLayout"] -match "http:\/\/(XXX|XX)\/" } | %{

        if($_.File.CheckOutStatus -eq "None") {

            $_.File.CheckOut($FALSE,$NULL)

            $_.Properties["PublishingPageLayout"] = $_.Properties["PublishingPageLayout"].Replace("http://XXX","http://XXXXX").Replace("http://XXXXX","http://XXX")

            $_.Update()

            $_.File.CheckIn("Update page layout via PowerShell",[Microsoft.SharePoint.SPCheckinType]::MajorCheckIn)

            write-host "File altered:" $_.File.ServerRelativeUrl

        }

        else {

            write-host "File CHECKEDOUT:" $_.File.ServerRelativeUrl

            Add-Content -Path "CheckedOutFiles.txt" -Value $_.File.ServerRelativeUrl

        }

    }

    $web.Dispose()

}

