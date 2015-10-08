/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package events;

import com.firebase.client.DataSnapshot;
import com.firebase.client.GenericTypeIndicator;

/**
 * Supertype for all {@link FirebaseEvent}s.
 * @param <T> type that {@link DataSnapshot} is parsed as.
**/
public class FirebaseEvent<T>{

  public final String key;
  public final T data;

  protected FirebaseEvent(String key, T data){
    this.key = key;
    this.data = data;
  }

  protected FirebaseEvent(DataSnapshot snapshot){
    this(snapshot.getKey(), snapshot.getValue(new GenericTypeIndicator<T>(){}));
  }

  /**
   * Does a deep equality check by delegating to {@code T.equals()}.
   */
  @Override
  public boolean equals(Object e){
    if (this.getClass().isInstance(e)){
      FirebaseEvent<?> other = (FirebaseEvent<?>) e;
      if ((this.key == null && other.key != null) || (other.key == null && this.key != null)){
        return false;
      }
      if ((this.data == null && other.data != null) || (other.data == null & this.data != null)){
        return false;
      }
      return (this.key == null || this.key.equals(other.key)) &&
          (this.data == null || this.data.equals(other.data));
    }
    return false;
  }

  @Override
  public String toString(){
    return "{event: " + this.getClass().getName() + ", key:" + key +
        ", data:" + (data == null ? "null" : data.toString()) + "}";
  }
}
