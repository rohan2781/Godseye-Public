from django.db import models

class TradeBook(models.Model):
    tradeNo = models.AutoField(primary_key=True)
    tradeTime = models.DateTimeField()
    accountId = models.TextField()
    instrument = models.TextField()
    symbol=models.TextField()
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

class Users(models.Model):
    userid=models.AutoField(primary_key=True)
    username=models.TextField()
    password=models.TextField()

    class Meta:
        db_table = 'Users'
        managed = False  # important: existing table
        app_label = 'godseye'

class Accounts(models.Model):
    accountId = models.AutoField(primary_key=True)
    name = models.TextField()
    userId = models.TextField()
    password = models.TextField()
    totp = models.TextField()
    appId = models.TextField()
    appName = models.TextField()
    appSecret = models.TextField()
    ration = models.TextField()
    redirect = models.TextField()
    enabled = models.TextField()
    twofa = models.TextField(null=True, blank=True)
    authToken = models.TextField(null=True, blank=True)
    ROC = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    master_class_instance_data = models.BinaryField(null=True, blank=True)  # This will hold serialized data of the class instance
    users=models.TextField()

    class Meta:
        db_table = 'Accounts'
        managed = False  # important: existing table
        app_label = 'godseye'
    

