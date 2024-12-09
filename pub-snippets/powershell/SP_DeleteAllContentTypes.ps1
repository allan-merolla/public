$web = get-spweb "http://XXXX/teams/XXXX-XXXXX"
foreach ($subWeb in $web.Webs)
{
  write-host $subWeb.Title;	
  foreach ($list in $subWeb.Lists)
  {
     if ($list.BaseType -ne "DocumentLibrary")
     {
        continue;
     }

     $id = "";
     foreach ($ct in $list.ContentTypes)
     {
        if ($ct.Name -eq "XXXX XXXX XXXX Report") 
        {
	    $id = $ct.Id;
		write-host -foregroundcolor yellow $ct.Name; 
        }
	if ($id -ne "")
	{
		$list.ContentTypes.Delete($id);
		$list.Update();
	}
     }
  }
  $subWeb.Dispose();
}
$webCt = $web.ContentTypes |? {$_.Name -eq "XXXXXX XXXXX XXXXX Report"};
$web.ContentTypes.Delete($webCt.Id);
$web.Update();
$web.Dispose();
