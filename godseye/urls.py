"""
URL configuration for godseye project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from . import views

# check login of accounts on top of every url and login if not already
urlpatterns = [
    # path('admin/', admin.site.urls),
    path('',views.index,name='index'),
    path('login',views.login,name='login'),
    path('home',views.home,name='home'),
    path('positions',views.positions,name='positions'),
    path('pnl',views.pnl,name='pnl'),
    path('squareoff/<str:id>',views.squareoff,name='squareoff'),
    path('squareoff_calls',views.squareoff_calls,name='squareoff_calls'),
    path('squareoff_puts',views.squareoff_puts,name='squareoff_puts'),
    path('get_ltp',views.get_ltp,name='get_ltp'),

]
