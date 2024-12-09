$webApplication = Get-SPWebApplication <Your_SharePoint_URL>

$collectionResult= @()
foreach ($SPsite in $webApplication.Sites)
{
    # Get the collection of webs
    foreach($SPweb in $SPsite.AllWebs)
    {
        $wfm = New-object Microsoft.SharePoint.WorkflowServices.WorkflowServicesManager($SPweb)
        $sub = $wfm.GetWorkflowSubscriptionService()

        foreach ($list in $SPweb.Lists)
        {
            if ($list.WorkflowAssociations -ne $null)
            {
                $collectionWF= @()

                foreach ($item in $list.Items)
                {
                    $runningWfs = $item.Workflows | ? {$_.InternalState -eq "Running"}

                    if ($runningWfs -ne $null)
                    {
                        foreach ($wf in $runningWfs)
                        {
                            $wfItem = New-Object System.Object
                            $wfItem | Add-Member -MemberType NoteProperty -Name "Name" -Value $wf.Name 
                            $wfItem | Add-Member -MemberType NoteProperty -Name "InstanceID" -Value $wf.ID.ToString()
                            $collectionWF += $wfItem
                        }
                    }
                }
            }

            $subscription = $sub.EnumerateSubscriptionsByList($list.ID)

            foreach ($s in $subscription)
            {
                $strIDs = ""
                $collectionWF | ? {$_.Name -eq $s.Name} | % {$strIDs = $strIDs + " " + $_.InstanceID.ToString()}
                $resItem = New-Object System.Object
                $resItem | Add-Member -MemberType NoteProperty -Name "Web URL" -Value $SPWeb.Url.ToString()
                $resItem | Add-Member -MemberType NoteProperty -Name "List" -Value $list.Title
                $resItem | Add-Member -MemberType NoteProperty -Name "WF Name" -Value $s.Name
                $resItem | Add-Member -MemberType NoteProperty -Name "Enabled" -Value $s.Enabled.ToString()
                $resItem | Add-Member -MemberType NoteProperty -Name "WF Count" -Value $collectionWF.Count.ToString()
                $resItem | Add-Member -MemberType NoteProperty -Name "WF Instances" -Value $strIDs
                $collectionWF += $resItem
                Write-host "Site:" $SPweb.Url "| List:" $list.Title "| Workflow Name:" $s.Name "| Enabled:" $s.Enabled
            }
        }
    }
}
$SPSite.Dispose()
