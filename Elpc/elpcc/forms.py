from django import forms
from django.contrib.auth.models import User

class SignupForm(forms.Form):
    username = forms.CharField(max_length=150, label='Username')
    email = forms.EmailField(label='Email Address') 
    password = forms.CharField(widget=forms.PasswordInput, label='Password')
    re_password = forms.CharField(widget=forms.PasswordInput, label='Retype Password')

class LoginForm(forms.Form):
    username = forms.CharField(max_length=150, label='Username')
    password = forms.CharField(widget=forms.PasswordInput, label='Password')    


