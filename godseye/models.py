from django.db import models

class TradeBook(models.Model):
    tradeNo = models.AutoField(primary_key=True)
    tradeTime = models.DateTimeField()
    accountId = models.TextField()
    instrument = models.TextField()
    side = models.TextField()
    price = models.DecimalField(max_digits=20, decimal_places=8)
    qty = models.DecimalField(max_digits=20, decimal_places=8)
    finalPrice = models.DecimalField(max_digits=20, decimal_places=8)
    orderId=models.TextField()
    expiry = models.DateField()

    class Meta:
        db_table = 'TradeBook'
        managed = False  # IMPORTANT since table already exists
        app_label = 'godseye'


    def __str__(self):
        return f"Trade {self.tradeNo} | {self.instrument} | {self.side}"

from django.db import models

class Accounts(models.Model):
    accountId = models.AutoField(primary_key=True)
    name = models.TextField()
    userId = models.TextField()
    password = models.TextField()
    year = models.TextField()
    appId = models.TextField()
    appName = models.TextField()
    appSecret = models.TextField()
    ration = models.TextField()
    redirect = models.TextField()
    enabled = models.TextField()
    twofa = models.TextField(null=True, blank=True)
    authToken = models.TextField(null=True, blank=True)
    ROC = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)

    class Meta:
        db_table = 'Accounts'
        managed = False  # important: existing table
        app_label = 'godseye'

    def __str__(self):
        return f"{self.accountId} - {self.name}"

