git config --global user.name "GitHub Actions"
git config --global user.email "actions@github.com"

git add dashboard/records.json data/records.json data/ghl_export.csv

git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "No changes to commit"
    exit 0
}

git commit -m "chore: update leads"

$success = $false
for ($i = 1; $i -le 3; $i++) {
    Write-Host "Push attempt $i of 3..."
    git pull --rebase origin main
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Rebase failed on attempt $i - aborting"
        git rebase --abort 2>$null
        Start-Sleep -Seconds 10
        continue
    }
    git push origin main
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Push succeeded on attempt $i"
        $success = $true
        break
    }
    Write-Host "Push failed on attempt $i, waiting 15s..."
    Start-Sleep -Seconds 15
}

if (-not $success) {
    Write-Host "ERROR: All push attempts failed"
    exit 1
}
