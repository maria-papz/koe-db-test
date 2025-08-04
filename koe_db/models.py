from django.db import models

from django.contrib.auth.models import (
    BaseUserManager,
    AbstractBaseUser,
    PermissionsMixin
)

class UserAccountManager(BaseUserManager):
    def create_user(self, email, password=None, **kwargs):
        if not email:
            raise ValueError('Users must have an email address')

        email = self.normalize_email(email)
        email = email.lower()

        user = self.model(
            email=email,
            **kwargs
        )

        user.set_password(password)
        user.save(using=self._db)

        return user

    def create_superuser(self, email, password=None, **kwargs):
        user = self.create_user(
            email,
            password=password,
            **kwargs
        )

        user.is_staff = True
        user.is_superuser = True
        user.save(using=self._db)

        return user


class UserAccount(AbstractBaseUser, PermissionsMixin):
    first_name = models.CharField(max_length=255)
    last_name = models.CharField(max_length=255)
    email = models.EmailField(unique=True, max_length=255)
    groups = models.ManyToManyField(
        'auth.Group',
        related_name='useraccount_set',
        blank=True,
        help_text='The groups this user belongs to.',
        verbose_name='groups',
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        related_name='useraccount_set',
        blank=True,
        help_text='Specific permissions for this user.',
        verbose_name='user permissions',
    )

    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    is_superuser = models.BooleanField(default=False)

    objects = UserAccountManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['first_name', 'last_name']

    def __str__(self):
        return self.email

class Unit(models.Model):
    name = models.CharField(max_length=50)
    symbol = models.CharField(max_length=10)
    description = models.TextField(blank=True, null=True)

class Category(models.Model):
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)

class Region(models.Model):
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)

class Country(models.Model):
    name = models.CharField(max_length=100)
    code = models.CharField(max_length=10, null=True)
    regions = models.ManyToManyField(Region, blank=True)

class Frequency(models.TextChoices):
    MINUTE = 'MINUTE', 'Per Minute'
    HOURLY = 'HOURLY', 'Hourly'
    DAILY = 'DAILY', 'Daily'
    WEEKLY = 'WEEKLY', 'Weekly'
    BIWEEKLY = 'BIWEEKLY', 'Biweekly'
    MONTHLY = 'MONTHLY', 'Monthly'
    BIMONTHLY = 'BIMONTHLY', 'Every 2 Months'
    QUARTERLY = 'QUARTERLY', 'Quarterly'
    TRIANNUAL = 'TRIANNUAL', 'Every 4 Months'
    SEMIANNUAL = 'SEMIANNUAL', 'Semiannual / Biannual'
    ANNUAL = 'ANNUAL', 'Annual'
    CUSTOM = 'CUSTOM', 'Custom / Other'

class Indicator(models.Model):
    name = models.CharField(max_length=255)
    code = models.CharField(max_length=50, unique=True, null=True)
    description = models.TextField()
    source = models.CharField(max_length=100, default='manual entry')
    data_frequency_years = models.FloatField(default=0.25)
    seasonally_adjusted = models.BooleanField(default=False)
    base_year = models.IntegerField(blank=True, null=True)
    is_custom = models.BooleanField(default=False)
    country = models.ForeignKey(Country, null=True, blank=True, on_delete=models.CASCADE)  # Optional for regional data
    region = models.ForeignKey(Region, null=True, blank=True, on_delete=models.CASCADE)  # Optional for regional data
    category = models.ForeignKey(Category, on_delete=models.SET_NULL, null=True)
    currentPrices = models.BooleanField(default=True)
    unit = models.ForeignKey(
        Unit,
        null=True,  # allow null so we can do the data migration
        blank=True,
        on_delete=models.SET_NULL
    )
    frequency = models.CharField(
        max_length=20,
        choices=Frequency.choices,
        default=Frequency.CUSTOM
    )
    other_frequency = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        help_text='Use this field if frequency is CUSTOM'
    )



class AccessLevel(models.Model):
    """Defines how an indicator's permissions are managed"""
    PUBLIC = 'public'
    UNRESTRICTED = 'unrestricted'
    ORGANIZATION = 'organization'
    RESTRICTED = 'restricted'
    ORG_FULL_PUBLIC = 'org_full_public'

    ACCESS_CHOICES = [
        (PUBLIC, 'Public Viewing'),
        (UNRESTRICTED, 'Unrestricted Access'),
        (ORGANIZATION, 'Organization Members'),
        (RESTRICTED, 'Restricted Access'),
        (ORG_FULL_PUBLIC, 'Organization Full Public')
    ]

    indicator = models.OneToOneField(
        Indicator,
        on_delete=models.CASCADE,
        related_name='access_level'
    )
    level = models.CharField(
        max_length=20,
        choices=ACCESS_CHOICES,
        default=PUBLIC,
        help_text='Controls default permissions for this indicator'
    )

    def __str__(self):
        return f"{self.indicator.name} - {self.get_level_display()}"

class IndicatorPermission(models.Model):
    """User-specific permissions for restricted indicators"""
    user = models.ForeignKey(UserAccount, on_delete=models.CASCADE)
    indicator = models.ForeignKey(
        Indicator,
        on_delete=models.CASCADE,
        related_name='user_permissions'
    )
    can_view = models.BooleanField(default=False)
    can_edit = models.BooleanField(default=False)
    can_delete = models.BooleanField(default=False)

    class Meta:
        unique_together = ['user', 'indicator']

    def __str__(self):
        return f"{self.user.email} - {self.indicator.code or self.indicator.id}"

class Data(models.Model):
    indicator = models.ForeignKey(Indicator, on_delete=models.CASCADE)
    date = models.DateField(blank=True, null=True)  # Date for specific time periods
    period = models.CharField(max_length=20, blank=True, null=True)  # Period description for non-date data
    value = models.DecimalField(max_digits=20, decimal_places=5, null=True)
    isEstimate = models.BooleanField(default=False)

class CustomTable(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField()
    indicators = models.ManyToManyField(Indicator, related_name='custom_tables')

class CustomIndicator(models.Model):
    """
    Represents a computed indicator that is derived from other indicators using a formula.
    """
    indicator = models.OneToOneField(Indicator, related_name="custom_formula", on_delete=models.CASCADE)
    formula = models.TextField()  # Store formula as a readable expression
    base_indicators = models.ManyToManyField(Indicator)

    def calculate_value(self, period):
        """
        Evaluates the formula dynamically using the values of base indicators.
        """
        values = {}

        # Fetch base indicator values for the given period
        for base_indicator in self.base_indicators.all():
            data = Data.objects.filter(indicator=base_indicator, period=period).first()
            if data:
                if data.value is not None:
                    values[f"@{base_indicator.code}"] = float(data.value)  # Prefix with '@' to match formula syntax
                else:
                    values[f"@{base_indicator.code}"] = None  # Handle missing values
        print(values)
        # Ensure all required values exist
        if None in values.values():
            return None  # Cannot compute if any base indicator is missing

        # Replace indicator names in the formula with their actual values
        formula_expr = self.formula
        for key, value in values.items():
            formula_expr = formula_expr.replace(key, str(value))  # Replace "@GDP" with "1000"

        # Evaluate the formula securely
        try:
            result = eval(formula_expr, {}, {})
            return result
        except Exception as e:
            print(f"Error evaluating formula: {e}")
            return None

class ActionLog(models.Model):
    ACTION_CHOICES = [
        ('DATA_UPDATE', 'Data Update'),
        ('INDICATOR_EDIT', 'Indicator Edit'),
        ('INDICATOR_CREATE', 'Indicator Create'),
        ('INDICATOR_DELETE', 'Indicator Delete'),
        ('FORMULA_UPDATE', 'Formula Update'),
    ]
    user = models.ForeignKey(
    UserAccount,
    on_delete=models.SET_NULL,
    null=True
    )
    indicator = models.ForeignKey(
        Indicator,
        on_delete=models.CASCADE
    )
    action_type = models.CharField(
        max_length=50,
        choices=ACTION_CHOICES
    )
    timestamp = models.DateTimeField(auto_now_add=True)
    details = models.JSONField()
    run = models.ForeignKey('WorkflowRun', on_delete=models.CASCADE, blank=True, null=True, related_name='action_logs')

    def __str__(self):
        return f"{self.action_type} on {self.indicator} by {self.user}"

class UserFavouriteTables(models.Model):
    user = models.ForeignKey(UserAccount, on_delete=models.CASCADE)
    tables = models.ManyToManyField(CustomTable)

class UserFavouriteIndicators(models.Model):
    user = models.ForeignKey(UserAccount, on_delete=models.CASCADE)
    indicators = models.ManyToManyField(Indicator)

class UserFollowsUser(models.Model):
    user = models.ForeignKey(UserAccount, on_delete=models.CASCADE, related_name='following')
    followed_users = models.ManyToManyField(UserAccount, related_name='followers')


class Workflow(models.Model):
    WORKFLOW_TYPES = [
        ('ECB', 'ECB Request'),
        ('CYSTAT', 'CyStat Request'),
        ('EUROSTAT', 'EuroStat Request'),
    ]

    name = models.CharField(max_length=255, null=False, default="Default Workflow Name")
    workflow_type = models.CharField(max_length=20, choices=WORKFLOW_TYPES)
    is_active = models.BooleanField(default=True)
    schedule_cron = models.CharField(max_length=100, default="0 0 1 * *")
    next_run = models.DateTimeField(null=True, blank=True)
    last_run = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} - {self.workflow_type}"


class WorkflowRun(models.Model):
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE, related_name="runs")
    run_time = models.DateTimeField(auto_now_add=True)  # Time of the workflow execution
    success = models.BooleanField(default=False)  # Whether the request was successful
    error_message = models.TextField(null=True, blank=True)  # Error details if the request failed
    status = models.CharField(max_length=20, default="PENDING")  # PENDING, IN_PROGRESS, COMPLETED, FAILED
    start_time = models.DateTimeField(null=True, blank=True)  # Start time of the workflow execution
    end_time = models.DateTimeField(null=True, blank=True)  # End time of the workflow execution
    def __str__(self):
        return f"Run for {self.workflow.indicator.name} at {self.run_time}"


class ECBRequest(models.Model):
    workflow = models.OneToOneField(Workflow, on_delete=models.CASCADE, related_name="ecb_request")
    table = models.CharField(max_length=100)
    parameters = models.CharField(max_length=255)
    frequency = models.CharField(max_length=20)
    indicator = models.ForeignKey(
        Indicator,
        on_delete=models.CASCADE,
        related_name="ecb_requests"
    )

    def __str__(self):
        return f"ECB Request for {self.indicator.name}"

class CyStatRequest(models.Model):
    workflow = models.OneToOneField(
        Workflow, on_delete=models.CASCADE, related_name="cystat_request"
    )
    url = models.URLField()
    request_body = models.JSONField()
    frequency = models.CharField(max_length=20)
    start_period = models.CharField(max_length=20)

    def __str__(self):
        return f"CyStat Request for {self.workflow.name}"


class CyStatIndicatorMapping(models.Model):
    """
    Explicitly connects indicators to the CyStat response keys.
    Each indicator corresponds uniquely to a combination of variable codes and indices.
    """
    cystat_request = models.ForeignKey(
        CyStatRequest, on_delete=models.CASCADE, related_name="indicator_mappings"
    )
    indicator = models.ForeignKey(
        Indicator, on_delete=models.CASCADE, related_name="cystat_mappings"
    )

    # Simple representation of the key-index mapping, for example:
    # {"GATEGORIES OF GOODS AND SERVICES": "1", "INDICATOR": "0"}
    key_indices = models.JSONField()

    class Meta:
        unique_together = ('cystat_request', 'indicator')

    def __str__(self):
        return f"{self.indicator.name}: {self.key_indices}"

class EuroStatRequest(models.Model):
    """
    Stores information about a Eurostat data request workflow.
    """
    workflow = models.OneToOneField(Workflow, on_delete=models.CASCADE, related_name="eurostat_request")
    url = models.URLField(help_text="The Eurostat API URL to fetch data from")
    frequency = models.CharField(max_length=20, help_text="Data frequency (e.g., 'Annual', 'Monthly', 'Quarterly')")

    def __str__(self):
        return f"Eurostat Request for {self.workflow.name}"

class EuroStatIndicatorMapping(models.Model):
    """
    Maps indicators to specific combinations of Eurostat dimension values.
    Stores only the essential information needed to extract the correct data points.
    """
    eurostat_request = models.ForeignKey(EuroStatRequest, on_delete=models.CASCADE, related_name="indicator_mappings")
    indicator = models.ForeignKey(Indicator, on_delete=models.CASCADE, related_name="eurostat_mappings")

    # Stores the mapping of dimensions to their values for this indicator
    # {
    #   "geo": "CY",
    #   "unit": "MIO_EUR",
    #   "indic_na": "B1G"
    # }
    dimension_values = models.JSONField(
        help_text="Mapping of dimension names to their selected values for this indicator"
    )

    class Meta:
        unique_together = ('eurostat_request', 'indicator')

    def __str__(self):
        return f"{self.indicator.name} in {self.eurostat_request.dataset_code}"
