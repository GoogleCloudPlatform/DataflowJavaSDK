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

/**
 * TODO: Super class for events which come with a {@code previousChildName}.
 * @param <T> type that {@link DataSnapshot} is parsed as.
 */
public class FirebaseChildEvent<T> extends FirebaseEvent<T> {

  public final String previousChildName;

  protected FirebaseChildEvent(String key, T data, String previousChildName) {
    super(key, data);
    this.previousChildName = previousChildName;
  }

  protected FirebaseChildEvent(DataSnapshot snapshot, String previousChildName) {
    super(snapshot);
    this.previousChildName = previousChildName;
  }


  @Override
  public boolean equals(Object e){
    if (this.getClass().isInstance(e)){
      FirebaseChildEvent<?> other = (FirebaseChildEvent<?>) e;
      if ((this.previousChildName == null && other.previousChildName != null) ||
          (other.previousChildName == null && this.previousChildName != null)){
        return false;
      }
      return (this.previousChildName == null  ||
          this.previousChildName.equals(other.previousChildName)) &&
          super.equals(e);
    }
    return false;
  }

  @Override
  public String toString(){
    return "{previousChildName: " + previousChildName + ", " + super.toString() + "}";
  }

}
