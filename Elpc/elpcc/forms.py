from django import forms
from django.contrib.auth.models import User

class Signup(forms.Form):
    username = forms.CharField(max_length=150, label='Username')
    