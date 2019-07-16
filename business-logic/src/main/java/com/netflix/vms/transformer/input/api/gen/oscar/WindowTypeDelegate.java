package com.netflix.vms.transformer.input.api.gen.oscar;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface WindowTypeDelegate extends HollowObjectDelegate {

    public String get_name(int ordinal);

    public boolean is_nameEqual(int ordinal, String testValue);

    public WindowTypeTypeAPI getTypeAPI();

}