/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.activity;

import java.util.concurrent.Callable;

/**
 * An Activity is a {@link Callable} that returns a boolean with a hook
 * ({@link #completed}) that gets called when the actual call finished.
 */
public interface Activity extends Callable<Boolean>
{
    /**
     * Called when the activity has completed
     *
     * @param wasSuccessful the result of {@link #call()}
     */
    public void     completed(boolean wasSuccessful);

    String getName();
}
