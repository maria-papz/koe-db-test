# forms.py

from django import forms
from django.apps import apps

def get_field_choices():
    field_choices = []
    app_models = apps.get_app_config('koe_db').get_models()
    for model in app_models:
        for field in model._meta.fields:
            if field.get_internal_type() in ['CharField', 'IntegerField', 'FloatField', 'DecimalField']:
                field_label = f"{model._meta.verbose_name} - {field.verbose_name}"
                field_name = f"{model._meta.model_name}__{field.name}"
                field_choices.append((field_name, field_label))
    return field_choices


class SimpleSearchForm(forms.Form):
    query = forms.CharField(label='Search', max_length=100)


class AdvancedSearchForm(forms.Form):
    BOOLEAN_CHOICES = [
        ('AND', 'AND'),
        ('OR', 'OR'),
        ('NOT', 'NOT')
    ]

    field = forms.ChoiceField(choices=get_field_choices(), required=True)
    value = forms.CharField(max_length=100, required=True)
    boolean = forms.ChoiceField(choices=BOOLEAN_CHOICES, required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['field'].choices = get_field_choices()
