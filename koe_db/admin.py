from django.contrib import admin
from django.apps import apps
from django.contrib.admin.sites import AlreadyRegistered

# from koe_db.models import UserActionLog
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Permission
# from koe_db.models import PermissionRequest

# class UserActionLogAdmin(admin.ModelAdmin):
#     list_display = ('user', 'action', 'model_name', 'timestamp', 'details')
#     list_filter = ('user', 'action', 'model_name', 'timestamp')
#     search_fields = ('user__username', 'model_name', 'details')

# admin.site.register(UserActionLog, UserActionLogAdmin)

app = apps.get_app_config('koe_db')
for model in app.get_models():
    try:
        admin.site.register(model)
    except AlreadyRegistered:
        pass
